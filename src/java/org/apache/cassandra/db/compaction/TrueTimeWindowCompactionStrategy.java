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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.SplittingTimeWindowCompactionWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.filter;

public class TrueTimeWindowCompactionStrategy extends AbstractCompactionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TrueTimeWindowCompactionStrategy.class);
    private static Range abnormalRange = new Range(Long.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE);

    private final TrueTimeWindowCompactionStrategyOptions options;
    protected volatile int estimatedRemainingTasks;
    private final Set<SSTableReader> sstables = new HashSet<>();
    private long lastExpiredCheck;
    private long highestWindowSeen;

    public TrueTimeWindowCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
        this.estimatedRemainingTasks = 0;
        this.options = new TrueTimeWindowCompactionStrategyOptions(options);
        if (!options.containsKey(AbstractCompactionStrategy.TOMBSTONE_COMPACTION_INTERVAL_OPTION) && !options.containsKey(AbstractCompactionStrategy.TOMBSTONE_THRESHOLD_OPTION))
        {
            disableTombstoneCompactions = true;
            logger.debug("Disabling tombstone compactions for TWCS");
        }
        else
            logger.debug("Enabling tombstone compactions for TWCS");
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        List<SSTableReader> previousCandidate = null;
        while (true) {
            List<SSTableReader> latestBucket = getNextBackgroundSSTables(gcBefore);

            if (latestBucket.isEmpty())
                return null;

            // Already tried acquiring references without success. It means there is a race with
            // the tracker but candidate SSTables were not yet replaced in the compaction strategy manager
            if (latestBucket.equals(previousCandidate)) {
                logger.warn("Could not acquire references for compacting SSTables {} which is not a problem per se," +
                                "unless it happens frequently, in which case it must be reported. Will retry later.",
                        latestBucket);
                return null;
            }

            LifecycleTransaction modifier = cfs.getTracker().tryModify(latestBucket, OperationType.COMPACTION);
            if (modifier != null) {
                if (latestBucket.size() == 1) {   // to be split
                    TimeWindowSplitCompactionTask task = new TimeWindowSplitCompactionTask(cfs, modifier, gcBefore);
                    task.setOptions(options);
                    return task;
                } else {
                    return new TimeWindowCompactionTask(cfs, modifier, gcBefore, options.ignoreOverlaps);
                }
            }
        previousCandidate = latestBucket;
        }
    }

    private static class TimeWindowSplitCompactionTask extends CompactionTask
    {
        private TrueTimeWindowCompactionStrategyOptions options;

        public TimeWindowSplitCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int gcBefore)
        {
            super(cfs, txn, gcBefore);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new SplittingTimeWindowCompactionWriter(cfs, directories, txn, nonExpiredSSTables, options);
        }

        public void setOptions(TrueTimeWindowCompactionStrategyOptions options) {
            this.options = options;
        }
    }

    /**
     *
     * @param gcBefore
     * @return
     */
    private synchronized List<SSTableReader> getNextBackgroundSSTables(final int gcBefore)
    {
        if (Iterables.isEmpty(cfs.getSSTables(SSTableSet.LIVE)))
            return Collections.emptyList();

        Set<SSTableReader> uncompacting = ImmutableSet.copyOf(filter(cfs.getUncompactingSSTables(), sstables::contains));

        // Find fully expired SSTables. Those will be included no matter what.
        Set<SSTableReader> expired = Collections.emptySet();

        if (System.currentTimeMillis() - lastExpiredCheck > options.expiredSSTableCheckFrequency)
        {
            logger.trace("TWCS expired check sufficiently far in the past, checking for fully expired SSTables");
            expired = CompactionController.getFullyExpiredSSTables(cfs, uncompacting, options.ignoreOverlaps ? Collections.emptySet() : cfs.getOverlappingLiveSSTables(uncompacting),
                                                                   gcBefore, options.ignoreOverlaps);
            lastExpiredCheck = System.currentTimeMillis();
        }
        else
        {
            logger.trace("TWCS skipping check for fully expired SSTables");
        }

        Set<SSTableReader> candidates = Sets.newHashSet(filterSuspectSSTables(uncompacting));

        List<SSTableReader> compactionCandidates = new ArrayList<>(getNextNonExpiredSSTables(Sets.difference(candidates, expired), gcBefore));
        if (compactionCandidates.size() > 1 && !expired.isEmpty())  //DO NOT include expired when trying splitting
        {
            logger.debug("Including expired sstables: {}", expired);
            compactionCandidates.addAll(expired);
        }

        return compactionCandidates;
    }

    private List<SSTableReader> getNextNonExpiredSSTables(Iterable<SSTableReader> nonExpiringSSTables, final int gcBefore)
    {
        List<SSTableReader> mostInteresting = getCompactionCandidates(nonExpiringSSTables);

        if (mostInteresting != null)
        {
            return mostInteresting;
        }

        // if there is no sstable to compact in standard way, try compacting single sstable whose droppable tombstone
        // ratio is greater than threshold.
        List<SSTableReader> sstablesWithTombstones = new ArrayList<>();
        for (SSTableReader sstable : nonExpiringSSTables)
        {
            if (worthDroppingTombstones(sstable, gcBefore))
                sstablesWithTombstones.add(sstable);
        }
        if (sstablesWithTombstones.isEmpty())
            return Collections.emptyList();

        return Collections.singletonList(Collections.min(sstablesWithTombstones, SSTableReader.sizeComparator));
    }

    private List<SSTableReader> getCompactionCandidates(Iterable<SSTableReader> candidateSSTables)
    {
        Pair<HashMultimap<Range, SSTableReader>, Long> buckets = getBuckets(candidateSSTables, options);
        // Update the highest window seen, if necessary
        if(buckets.right > this.highestWindowSeen)
            this.highestWindowSeen = buckets.right;

        updateEstimatedCompactionsByTasks(buckets.left);
        List<SSTableReader> mostInteresting = newestBucket(buckets.left,
                                                           cfs.getMinimumCompactionThreshold(),
                                                           cfs.getMaximumCompactionThreshold(),
                                                           options,
                                                            this.highestWindowSeen);
        if (!mostInteresting.isEmpty())
            return mostInteresting;
        return null;
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
     * Find the lowest and highest timestamps in a given timestamp/unit pair
     * Returns milliseconds, caller should adjust accordingly
     */
    public static Pair<Long,Long> getWindowBoundsInMillis(TimeUnit windowTimeUnit, int windowTimeSize, long timestampInMillis)
    {
        long lowerTimestamp;
        long upperTimestamp;
        long timestampInSeconds = TimeUnit.SECONDS.convert(timestampInMillis, TimeUnit.MILLISECONDS);

        switch(windowTimeUnit)
        {
            case MINUTES:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (60L * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (60L * (windowTimeSize - 1L))) + 59L;
                break;
            case HOURS:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (3600L * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (3600L * (windowTimeSize - 1L))) + 3599L;
                break;
            case DAYS:
            default:
                lowerTimestamp = timestampInSeconds - ((timestampInSeconds) % (86400L * windowTimeSize));
                upperTimestamp = (lowerTimestamp + (86400L * (windowTimeSize - 1L))) + 86399L;
                break;
        }

        return Pair.create(TimeUnit.MILLISECONDS.convert(lowerTimestamp, TimeUnit.SECONDS),
                           TimeUnit.MILLISECONDS.convert(upperTimestamp, TimeUnit.SECONDS));

    }

    private static class Range implements java.io.Serializable, Comparable<Range>  {
        public long max;
        public long min;
        public int span;

        Range(long min, long max, long timeWindow)
        {
            this.min = min;
            this.max = max;
            if (timeWindow == Long.MIN_VALUE)
                this.span = Integer.MAX_VALUE;
            else
                this.span = (int) ((max - min) / timeWindow + 1);
            //logger.trace("new range {}-{}, span {}, timewindow {}", min, max, span, timeWindow);
        }

        @Override
        public int compareTo(Range o) {
            long x = this.max + this.min;
            long y = o.max + o.min;
            return (x < y) ? -1 : ((x == y && this.max == o.max && this.span == o.span) ? 0 : 1);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Range) {
                return compareTo((Range) obj) == 0;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 31 * Long.hashCode(min) + Long.hashCode(max) + Long.hashCode(span);
            //logger.debug("range hashcode called , return {}", hash);
            return hash;
        }
    }

    /**
     * Group files with similar max timestamp into buckets.
     *
     * @param files pairs consisting of a file and its min timestamp
     * @param options
     * @return A pair, where the left element is the bucket representation (map of timestamp to sstablereader), and the right is the highest timestamp seen
     */
    @VisibleForTesting
    static Pair<HashMultimap<Range, SSTableReader>, Long> getBuckets(Iterable<SSTableReader> files, TrueTimeWindowCompactionStrategyOptions options)
    {
        HashMultimap<Range, SSTableReader> buckets = HashMultimap.create();

        long maxTimestamp = 0;
        long minSplitTimeInMills = System.currentTimeMillis() - options.minSStableSplitWindow * options.timeWindowInMillis;

        // Create hash map to represent buckets
        // For each sstable, add sstable to the time bucket
        // Where the bucket is the file's max timestamp rounded to the nearest window bucket
        for (SSTableReader f : files)
        {
            assert TimeWindowCompactionStrategyOptions.validTimestampTimeUnits.contains(options.timestampResolution);
            long tStampMax = TimeUnit.MILLISECONDS.convert(f.getMaxTimestamp(), options.timestampResolution);
            long tStampMin = TimeUnit.MILLISECONDS.convert(f.getMinTimestamp(), options.timestampResolution);
            Pair<Long,Long> boundsMax = getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, tStampMax);
            Pair<Long,Long> boundsMin = getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, tStampMin);
            Pair<Long,Long> boundsSpit = getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, minSplitTimeInMills);

            Range range = new Range(boundsMin.left, boundsMax.left, options.timeWindowInMillis);
            // real-time tw OR span > 2, try to compact big enough then split later.
            if (range.span <= 2 ||
                    (f.onDiskLength() > options.sstableSplitThreshold && boundsMax.left > boundsSpit.left)) {
                buckets.put(range, f);
            } else if (boundsMax.left > boundsSpit.left) {
                buckets.put(abnormalRange, f);
            } else {
                buckets.put(new Range(Long.MIN_VALUE, boundsSpit.left, options.timeWindowInMillis), f);
            }
            if (boundsMax.left != Long.MAX_VALUE && boundsMax.left > maxTimestamp)
                maxTimestamp = boundsMax.left;
        }

        logger.trace("buckets {}, max timestamp {}", buckets, maxTimestamp);
        return Pair.create(buckets, maxTimestamp);
    }

    private void updateEstimatedCompactionsByTasks(HashMultimap<Range, SSTableReader> tasks)
    {
        int n = 0;
        long now = this.highestWindowSeen;

        for(Range range : tasks.keySet())
        {
            Long key = range.max;
            // For current window, make sure it's compactable
            if (key.compareTo(now) >= 0 && tasks.get(range).size() >= cfs.getMinimumCompactionThreshold())
                n++;
            else if (key.compareTo(now) < 0 && tasks.get(range).size() >= 2)
                n++;
        }
        this.estimatedRemainingTasks = n;
    }


    /**
     * @param buckets list of buckets, sorted from newest to oldest, from which to return the newest bucket within thresholds.
     * @param minThreshold minimum number of sstables in a bucket to qualify.
     * @param maxThreshold maximum number of sstables to compact at once (the returned bucket will be trimmed down to this).
     * @param now
     * @return a bucket (list) of sstables to compact.
     */
    @VisibleForTesting
    static List<SSTableReader> newestBucket(HashMultimap<Range, SSTableReader> buckets, int minThreshold, int maxThreshold,
                                            TrueTimeWindowCompactionStrategyOptions options, long now)
    {
        // If the current bucket has at least minThreshold SSTables, choose that one.
        // For any other bucket, at least 2 SSTables is enough.
        // In any case, limit to maxThreshold SSTables.

        SizeTieredCompactionStrategyOptions stcsOptions = options.stcsOptions;
        TreeSet<Range> allKeys = new TreeSet<>(buckets.keySet());

        Iterator<Range> it = allKeys.descendingIterator();
        while(it.hasNext())
        {
            Range range = it.next();

            Set<SSTableReader> bucket = buckets.get(range);
            logger.trace("Key {} - {} ", range.min, now);
            if (bucket.size() >= minThreshold && range.span <= 2 && range.max >= now)
            {
                // If we're in the newest bucket, we'll use STCS to prioritize sstables
                List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(bucket);
                List<List<SSTableReader>> stcsBuckets = SizeTieredCompactionStrategy.getBuckets(pairs, stcsOptions.bucketHigh, stcsOptions.bucketLow, stcsOptions.minSSTableSize);
                logger.debug("Using STCS compaction for first window of bucket: data files {} , options {}", pairs, stcsOptions);
                List<SSTableReader> stcsInterestingBucket = SizeTieredCompactionStrategy.mostInterestingBucket(stcsBuckets, minThreshold, maxThreshold);

                // If the tables in the current bucket aren't eligible in the STCS strategy, we'll skip it and look for other buckets
                if (!stcsInterestingBucket.isEmpty())
                    return stcsInterestingBucket;
            }
            else if (bucket.size() >= 2)
            {
                logger.debug("bucket size {} >= 2 and not in current bucket, compacting what's here: {}", bucket.size(), bucket);
                boolean excludeLargest = false;
                if (range.span<=2 && range.max >= now) {
                    continue;
                }
                if (range.span == 1 && range.max < now) {
                    excludeLargest = true;
                }

                List<SSTableReader>  ret = trimToThreshold(bucket, maxThreshold, excludeLargest, options.maxSStableSizeThreshold);
                if (ret != null) {
                    return ret;
                }
            }
            else if (bucket.size() == 1 && range.span > 1)
            {
                boolean excludeSplit = (range.min == Long.MIN_VALUE && range.max != Long.MAX_VALUE)
                        || bucket.iterator().next().onDiskLength() < options.sstableSplitThreshold;
                if (excludeSplit) {
                    continue;
                }
                logger.debug("bucket size == 1 and span {} not in current bucket, compacting(splitting) what's here: {}", range.span, bucket);
                return ImmutableList.copyOf(Iterables.limit(bucket, 1));
            }
            else
            {
                logger.trace("No compaction necessary for bucket key {}, span {}", range.min, range.span);
            }
        }
        return Collections.<SSTableReader>emptyList();
    }

    /**
     * @param bucket set of sstables
     * @param maxThreshold maximum number of sstables in a single compaction task.
     * @param excludeLargest
     * @param maxSStableSizeThreshold
     * @return A bucket trimmed to the maxThreshold newest sstables.
     */
    @VisibleForTesting
    static List<SSTableReader> trimToThreshold(Set<SSTableReader> bucket, int maxThreshold, boolean excludeLargest, long maxSStableSizeThreshold)
    {
        List<SSTableReader> ssTableReaders = new ArrayList<>(bucket);

        // Trim the largest sstables off the end to meet the maxThreshold
        Collections.sort(ssTableReaders, SSTableReader.sizeComparator);
        if (excludeLargest && ssTableReaders.get(ssTableReaders.size() - 1).onDiskLength() >= maxSStableSizeThreshold) {
            excludeLargest = true;
        } else {
            excludeLargest = false;
        }

        if (excludeLargest && ssTableReaders.size() <= 2) {
            return null;
        }

        return ImmutableList.copyOf(Iterables.limit(ssTableReaders,
                Math.max(maxThreshold, ssTableReaders.size() - (excludeLargest ? 1:0 ))));
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        Iterable<SSTableReader> filteredSSTables = filterSuspectSSTables(sstables);
        if (Iterables.isEmpty(filteredSSTables))
            return null;
        LifecycleTransaction txn = cfs.getTracker().tryModify(filteredSSTables, OperationType.COMPACTION);
        if (txn == null)
            return null;
        return Collections.singleton(new TimeWindowCompactionTask(cfs, txn, gcBefore, options.ignoreOverlaps));
    }

    @Override
    @SuppressWarnings("resource") // transaction is closed by AbstractCompactionTask::execute
    public synchronized AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        assert !sstables.isEmpty(); // checked for by CM.submitUserDefined

        LifecycleTransaction modifier = cfs.getTracker().tryModify(sstables, OperationType.COMPACTION);
        if (modifier == null)
        {
            logger.debug("Unable to mark {} for compaction; probably a background compaction got to it first.  You can disable background compactions temporarily if this is a problem", sstables);
            return null;
        }

        return new TimeWindowCompactionTask(cfs, modifier, gcBefore, options.ignoreOverlaps).setUserDefined(true);
    }

    public int getEstimatedRemainingTasks()
    {
        return this.estimatedRemainingTasks;
    }

    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }


    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = AbstractCompactionStrategy.validateOptions(options);
        uncheckedOptions = TrueTimeWindowCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        return uncheckedOptions;
    }

    public String toString()
    {
        return String.format("NewTimeWindowCompactionStrategy[%s/%s]",
                cfs.getMinimumCompactionThreshold(),
                cfs.getMaximumCompactionThreshold());
    }
}
