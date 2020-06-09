/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static com.google.common.collect.Iterables.filter;

public class TimeTieredCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeTieredCompactionStrategy.class);

    private final TimeTieredCompactionStrategyOptions options;
    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private long lastExpiredCheck;
    // pause for compaction for old Bucket if it has more than 2
    private final SizeTieredCompactionStrategyOptions stcsOptions;

    public TimeTieredCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.options = new TimeTieredCompactionStrategyOptions(options);
        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.trace("Disabling tombstone compactions for TTCS");
        }
        else
            logger.trace("Enabling tombstone compactions for TTCS");
        this.stcsOptions = new SizeTieredCompactionStrategyOptions(options);
    }

    @Override
    @SuppressWarnings("resource")
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        List<SSTableReader> previousCandidate = null;
        while (true)
        {
            List<SSTableReader> latestBucket = getNextBackgroundSSTables(gcBefore);

            if (latestBucket.isEmpty())
                return null;

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
            if (latestBucket.equals(previousCandidate))
            {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                            "unless it happens frequently, in which case it must be reported. Will retry later.",
                            latestBucket);
                return null;
            }

            LifecycleTransaction modifier = cfs.getTracker().tryModify(latestBucket, OperationType.COMPACTION);
            if (modifier != null)
                return new CompactionTask(cfs, modifier, gcBefore);
            previousCandidate = latestBucket;
        }
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    private synchronized List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        Set<SSTableReader> uncompacting;
        synchronized (sstables)
        {
            if (sstables.isEmpty())
                return Collections.emptyList();

            uncompacting = ImmutableSet.copyOf(filter(cfs.getUncompactingSSTables(), sstables::contains));
        }

        Set<SSTableReader> expired = Collections.emptySet();
        // we only check for expired sstables every 10 minutes (by default) due to it being an expensive operation
        if (System.currentTimeMillis() - lastExpiredCheck > options.expiredSSTableCheckFrequency)
        {
            // Find fully expired SSTables. Those will be included no matter what.
            expired = CompactionController.getFullyExpiredSSTables(cfs, uncompacting, cfs.getOverlappingLiveSSTables(uncompacting), gcBefore);
            lastExpiredCheck = System.currentTimeMillis();
        }
        Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));

        List<SSTableReader> compactionCandidates = new ArrayList<>(getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));
        if (!expired.isEmpty())
        {
            logger.trace("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
        }
        return compactionCandidates;
    }

    private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        int base = cfs.getMinimumCompactionThreshold();
        long now = getNow();
        List<SSTableReader> mostInteresting = getCompactionCandidates(nonExpiringSSTables, now, base);
        // compact sstable in timeWindow first
        if (mostInteresting != null)
        {
            return mostInteresting;
        }


        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = Lists.newArrayList();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.min(sstablesWithTombstones, SSTableReader.sizeComparator));
    }

    private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables, long now, int base)
    {
        Set<SSTableReader> buffer = new HashSet<>();
        List<List<SSTableReader>> buckets = getBuckets(createSSTableAndTimeWindowPairs(candidateSSTables), options.sstableWindowSizeMin, base, now, options.sstableWindowSizeMax, buffer);
        logger.debug("Compaction buckets are {}", buckets);
        long maxWindowTimeStamp = getMaxWindowTime(now, options.sstableWindowSizeMin, options.sstableWindowSizeMax, base);
        updateEstimatedCompactionsByTasks(buckets, buffer, maxWindowTimeStamp);

        List<SSTableReader> mostInteresting = newestBucket(buckets,
                                                           cfs.getMinimumCompactionThreshold(),
                                                           cfs.getMaximumCompactionThreshold(),
                                                           stcsOptions);
        if (!mostInteresting.isEmpty())
            return mostInteresting;

        List<SSTableReader> oldBucket = oldestBucket(buckets, maxWindowTimeStamp);
        if (!oldBucket.isEmpty())
            return oldBucket;

        if (buffer.size() > options.bufferCompactionSize)
        {
            List<Pair<List<SSTableReader>, Pair<Long, Long>>> disOrderedBucket = getBucketsInBuffer(createSSTableAndTimeWindowPairs(buffer));
            List<SSTableReader> favoriteBucket = getFavorBucket(disOrderedBucket, options.removeWindowSize, options.sstableWindowSizeMax, maxWindowTimeStamp, buckets);
            if (!favoriteBucket.isEmpty())
                return favoriteBucket;
        }

        return null;
    }

    @VisibleForTesting
    static List<SSTableReader> oldestBucket(List<List<SSTableReader>> buckets, long maxWindowTime)
    {
        // If the "incoming window" has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.
        List<SSTableReader> bucket = Lists.newArrayList();
        int max = 1;
        for (int i = buckets.size() - 1; i >= 0; i--)
        {
            if (buckets.get(i).size() > max && buckets.get(i).get(0).getMaxTimestamp() < maxWindowTime)
            {
                bucket = buckets.get(i);
                max = buckets.get(i).size();
            }
        }
        if (bucket.size() >= 2)
            return bucket;
        return Collections.emptyList();
    }

    @VisibleForTesting
    static List<SSTableReader> getFavorBucket(List<Pair<List<SSTableReader>, Pair<Long, Long>>> disOrderedBucket,long removeWindowSize, long maxWindowSize, long maxWindowTime, List<List<SSTableReader>> buckets)
    {

        long maxSSTableNum = 0;
        Pair<List<SSTableReader>, Pair<Long, Long>> longestBucket = null;

        for (Pair<List<SSTableReader>, Pair<Long, Long>> bucketWithRange: disOrderedBucket)
        {
            if (bucketWithRange.right.right > maxWindowTime)
                continue;
            Pair<Long, Long> timeRange = longestBucket.right;
            long timeInterval = timeRange.right - timeRange.left;
            if (bucketWithRange.left.size() >= maxSSTableNum && timeInterval < removeWindowSize)
            {
                longestBucket = bucketWithRange;
                maxSSTableNum = bucketWithRange.left.size();
            }
        }
        if (longestBucket != null)
        {
            long timeInterval = longestBucket.right.right - longestBucket.right.left;
            if (timeInterval > removeWindowSize)
                return longestBucket.left;
            else {
                Pair<Long, Long> bufferPair = Pair.create(longestBucket.right.left / maxWindowSize * maxWindowSize, (1 + longestBucket.right.right /maxWindowSize) * maxWindowSize);
                List<SSTableReader> all = longestBucket.left;
                for (List<SSTableReader> sstablelist: buckets) {
                    if (!sstablelist.isEmpty()) {
                        List<Pair<SSTableReader, Pair<Long, Long>>> sstable = createSSTableAndTimeWindowPairs(sstablelist);
                        for (Pair<SSTableReader, Pair<Long, Long>> ssTableReader: sstable) {
                            if (include(bufferPair, ssTableReader.right))
                                all.add(ssTableReader.left);
                        }
                    }
                }
                return all;
            }
        }
        return Collections.emptyList();
    }

    @VisibleForTesting
    static <T> List<Pair<List<T>, Pair<Long, Long>>> getBucketsInBuffer(Collection<Pair<T, Pair<Long, Long>>> files)
    {
        final List<Pair<T, Pair<Long, Long>>> sortedFiles = Lists.newArrayList(files);
        Collections.sort(sortedFiles, Comparator.comparing(p -> p.right.left));

        List<Pair<List<T>, Pair<Long, Long>>> buckets = Lists.newArrayList();


        int i = 0;
        while (i < sortedFiles.size()) {
            Pair<Long, Long> timeSpan = sortedFiles.get(i).right;
            List<T> bucket = Lists.newArrayList();
            int j = i + 1;
            while (j < sortedFiles.size() && intersect(sortedFiles.get(j).right, timeSpan)) {
                long min = Math.min(timeSpan.left, sortedFiles.get(j).right.left);
                long max = Math.max(timeSpan.right, sortedFiles.get(j).right.right);
                timeSpan = Pair.create(min, max);
                j++;
            }
            while (i < j) {
                bucket.add(sortedFiles.get(i).left);
                i++;
            }
            buckets.add(Pair.create(bucket, timeSpan));
        }

        return buckets;
    }

    @VisibleForTesting
    static boolean include(Pair<Long, Long> timeRange, Pair<Long, Long> sstableRange) {
        if (timeRange.right >= sstableRange.right && timeRange.left <= sstableRange.left)
            return true;
        return false;
    }

    @VisibleForTesting
    static boolean intersect(Pair<Long, Long> timeRange, Pair<Long, Long> sstableRange) {
        if (timeRange.right >= sstableRange.left && timeRange.left <= sstableRange.right)
            return true;
        return false;
    }

    @VisibleForTesting
    static Pair<Long, Long> newRange(Pair<Long, Long> timeRange, Pair<Long, Long> sstableRange) {
        long min = timeRange.left < sstableRange.left ? timeRange.left : sstableRange.left;
        long max = timeRange.right > sstableRange.right ? timeRange.right : sstableRange.right;
        return Pair.create(min, max);
    }

    /**
     * Gets the timestamp that TimeTieredCompactionStrategy considers to be the "current time".
     * @return the maximum timestamp across all SSTables.
     * @throws NoSuchElementException if there are no SSTables.
     */
    private long getNow()
    {
        // no need to convert to collection if had an Iterables.max(), but not present in standard toolkit, and not worth adding
        List<SSTableReader> list = new ArrayList<>();
        Iterables.addAll(list, cfs.getSSTables(SSTableSet.LIVE));
        if (list.isEmpty())
            return 0;
        return Collections.max(list, (o1, o2) -> Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp()))
                          .getMaxTimestamp();

        // old getNow returns the maxTimeStamp of the sstables
        // now we return currentTimeMillis
        // return System.currentTimeMillis() * 1000;
    }


    public static List<Pair<SSTableReader, Pair<Long, Long>>> createSSTableAndTimeWindowPairs(Iterable<SSTableReader> sstables)
    {
        List<Pair<SSTableReader, Pair<Long, Long>>> sstableTimeWindowPairs = Lists.newArrayListWithCapacity(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            sstableTimeWindowPairs.add(Pair.create(sstable, Pair.create(sstable.getMinTimestamp(), sstable.getMaxTimestamp())));
        return sstableTimeWindowPairs;
    }

    @Override
    public synchronized void addSSTable(SSTableReader sstable)
    {
        sstables.add(sstable);
    }

    @Override
    public synchronized void removeSSTable(SSTableReader sstable)
    {
        sstables.remove(sstable);
    }

    /**
     * A target time span used for bucketing SSTables based on timestamps.
     */
    private static class TimeWindow
    {
        // How big a range of timestamps fit inside the target.
        public final long size;
        // A timestamp t hits the target iff t / size == divPosition.
        public final long divPosition;

        public final long maxWindowSize;

        public TimeWindow(long size, long divPosition, long maxWindowSize)
        {
            this.size = size;
            this.divPosition = divPosition;
            this.maxWindowSize = maxWindowSize;
        }

        /**
         * Compares the target to a timestamp.
         * @param timestamp the timestamp to compare.
         * @return a negative integer, zero, or a positive integer as the target lies before, covering, or after than the timestamp.
         */
        public int compareToTimestamp(long timestamp)
        {
            return Long.compare(divPosition, timestamp / size);
        }

        /**
         * Tells if the timestamp hits the target.
         * @param timeRange the timestamp to test.
         * @return <code>true</code> iff timestamp / size == divPosition.
         */
        public boolean onTimeWindow(Pair<Long, Long> timeRange)
        {
            return compareToTimestamp(timeRange.left) == 0 && compareToTimestamp(timeRange.right) == 0;
        }

        /**
         * Gets the next target, which represents an earlier time span.
         * @param base The number of contiguous targets that will have the same size. Targets following those will be <code>base</code> times as big.
         * @return
         */
        public TimeWindow nextTimeWindow(int base)
        {
            if (divPosition % base > 0 || size * base > maxWindowSize)
                return new TimeWindow(size, divPosition - 1, maxWindowSize);
            else
                return new TimeWindow(size * base, divPosition / base - 1, maxWindowSize);
        }
    }


    /**
     * Group files with similar max timestamp into buckets. Files with recent max timestamps are grouped together into
     * buckets designated to short timespans while files with older timestamps are grouped into buckets representing
     * longer timespans.
     * @param files pairs consisting of a file and its max timestamp
     * @param timeUnit
     * @param base
     * @param now
     * @return a list of buckets of files. The list is ordered such that the files with newest timestamps come first.
     *         Each bucket is also a list of files ordered from newest to oldest.
     */
    @VisibleForTesting
    static <T> List<List<T>> getBuckets(Collection<Pair<T, Pair<Long, Long>>> files, long timeUnit, int base, long now, long maxWindowSize, Set<T> buffer)
    {
        // Sort files by age. Newest first.
        final List<Pair<T, Pair<Long, Long>>> sortedFiles = Lists.newArrayList(files);
        Collections.sort(sortedFiles, Collections.reverseOrder(new Comparator<Pair<T, Pair<Long,Long>>>()
        {
            public int compare(Pair<T,Pair<Long, Long>> p1, Pair<T,Pair<Long, Long>> p2)
            {
                return p1.right.right.compareTo(p2.right.right);
            }
        }));

        List<List<T>> buckets = Lists.newArrayList();
        TimeWindow timeWindow = getInitialTimeWindow(now, timeUnit, maxWindowSize);
        PeekingIterator<Pair<T, Pair<Long, Long>>> it = Iterators.peekingIterator(sortedFiles.iterator());

        outerLoop:
        while (it.hasNext())
        {
            while (!timeWindow.onTimeWindow(it.peek().right))
            {
                // if the file is across the timeWindow
                if (timeWindow.compareToTimestamp(it.peek().right.right) != timeWindow.compareToTimestamp(it.peek().right.left)
                   || (timeWindow.compareToTimestamp(it.peek().right.left) < 0))
                {
                    buffer.add(it.next().left);
                    if (!it.hasNext())
                        break outerLoop;
                }
                else // If the file is too old for the target, switch targets.
                    timeWindow = timeWindow.nextTimeWindow(base);
            }
            List<T> bucket = Lists.newArrayList();
            while (timeWindow.onTimeWindow(it.peek().right))
            {
                bucket.add(it.next().left);

                if (!it.hasNext())
                    break;
            }
            buckets.add(bucket);
        }

        return buckets;
    }

    @VisibleForTesting
    static TimeWindow getInitialTimeWindow(long now, long timeUnit, long maxWindowSize)
    {
        return new TimeWindow(timeUnit, now / timeUnit, maxWindowSize);
    }

    static long getMaxWindowTime(long now, long timeUnit, long maxWindowSize, int base) {
        TimeWindow timeWindow = getInitialTimeWindow(now, timeUnit, maxWindowSize);
        long timeTarget = timeWindow.size;
        while (timeWindow.divPosition >= 0) {
            if (timeTarget < maxWindowSize)
            {
                timeWindow = timeWindow.nextTimeWindow(base);
                timeTarget = timeWindow.size;
            }
            else
                return timeWindow.size * (timeWindow.divPosition + 1);
        }
        return 0;
    }

    private void updateEstimatedCompactionsByTasks(List<List<SSTableReader>> tasks, Set<SSTableReader> buffer, long maxWindowTime)
    {
        int n = 0;
        for (List<SSTableReader> bucket : tasks)
        {
            if (bucket.size() >= 2 && bucket.get(0).getMaxTimestamp() <= maxWindowTime)
            {
                n++;
            }
            else {
                for (List<SSTableReader> stcsBucket : getSTCSBuckets(bucket, stcsOptions))
                    if (stcsBucket.size() >= cfs.getMinimumCompactionThreshold())
                        n += Math.ceil((double)stcsBucket.size() / cfs.getMaximumCompactionThreshold());
            }
        }
        estimatedRemainingTasks = n;
        if (buffer.size() > options.bufferCompactionSize)
            estimatedRemainingTasks++;
        cfs.getCompactionStrategyManager().compactionLogger.pending(this, n);
    }


    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @return a bucket (list) of sstables to compact.
     */
    @VisibleForTesting
    static List<SSTableReader> newestBucket(List<List<SSTableReader>> buckets, int minThreshold, int maxThreshold, SizeTieredCompactionStrategyOptions stcsOptions)
    {
        // If the "incoming window" has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.
        // long maxWindowTime = getMaxWindowTime(now, baseTime, maxWindowSize, minThreshold);
        for (List<SSTableReader> bucket : buckets)
        {
            if (bucket.size() >= minThreshold)
            {
                List<SSTableReader> stcsSSTables = getSSTablesForSTCS(bucket, minThreshold, maxThreshold, stcsOptions);
                if (!stcsSSTables.isEmpty())
                    return stcsSSTables;
            }
        }
        return Collections.emptyList();
    }

    private static List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables, int minThreshold, int maxThreshold, SizeTieredCompactionStrategyOptions stcsOptions)
    {
        List<SSTableReader> s = SizeTieredCompactionStrategy.mostInterestingBucket(getSTCSBuckets(sstables, stcsOptions), minThreshold, maxThreshold);
        logger.debug("Got sstables {} for TTCS from {}", s, sstables);
        return s;
    }

    private static List<List<SSTableReader>> getSTCSBuckets(Collection<SSTableReader> sstables, SizeTieredCompactionStrategyOptions stcsOptions)
    {
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(sstables));
        return SizeTieredCompactionStrategy.getBuckets(pairs, stcsOptions.bucketHigh, stcsOptions.bucketLow, stcsOptions.minSSTableSize);
    }

    @Override
    @SuppressWarnings("resource")
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Collections.<AbstractCompactionTask>singleton(new CompactionTask(cfs, txn, gcBefore));
    }

    @Override
    @SuppressWarnings("resource")
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction modifier = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (modifier == null)
        {
            logger.trace("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new CompactionTask(cfs, modifier, gcBefore).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    /**
     * DTCS should not group sstables for anticompaction - this can mix new and old data
     */
    @Override
    public Collection<Collection<SSTableReader>> groupSSTablesForAntiCompaction(Collection<SSTableReader> sstablesToGroup)
    {
        Collection<Collection<SSTableReader>> groups = new ArrayList<>();
        for (SSTableReader sstable : sstablesToGroup)
        {
            groups.add(Collections.singleton(sstable));
        }
        return groups;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TimeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        uncheckedOptions = TimeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }

    public CompactionLogger.Strategy strategyLogger()
    {
        return new CompactionLogger.Strategy()
        {
            public JsonNode sstable(SSTableReader sstable)
            {
                ObjectNode node = JsonNodeFactory.instance.objectNode();
                node.put("min_timestamp", sstable.getMinTimestamp());
                node.put("max_timestamp", sstable.getMaxTimestamp());
                return node;
            }

            public JsonNode options()
            {
                ObjectNode node = JsonNodeFactory.instance.objectNode();
                TimeUnit resolution = TimeTieredCompactionStrategy.this.options.timestampResolution;
                node.put(TimeTieredCompactionStrategyOptions.TIMESTAMP_RESOLUTION_KEY,
                         resolution.toString());
                node.put(TimeTieredCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_MIN_KEY,
                         resolution.toSeconds(TimeTieredCompactionStrategy.this.options.sstableWindowSizeMin));
                node.put(TimeTieredCompactionStrategyOptions.COMPACTION_WINDOW_SIZE_MAX_KEY,
                         resolution.toSeconds(TimeTieredCompactionStrategy.this.options.sstableWindowSizeMax));
                node.put(TimeTieredCompactionStrategyOptions.REMOVE_SIZE_KEY,
                         resolution.toSeconds(TimeTieredCompactionStrategy.this.options.removeWindowSize));
                return node;
            }
        };
    }

    public String toString()
    {
        return String.format("TimeTieredCompactionStrategy[%s/%s]",
                cfs.getMinimumCompactionThreshold(),
                cfs.getMaximumCompactionThreshold());
    }
}
