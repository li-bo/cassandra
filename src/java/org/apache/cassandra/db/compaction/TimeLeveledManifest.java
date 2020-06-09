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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import javax.swing.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class TimeLeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(TimeLeveledManifest.class);

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    private static final int MAX_COMPACTING_L0 = 32;
    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    private static final int NO_COMPACTION_LIMIT = 25;
    // allocate enough generations for a PB of data, with a 1-MB sstable size.  (Note that if maxSSTableSize is
    // updated, we will still have sstables of the older, potentially smaller size.  So don't make this
    // dependent on maxSSTableSize.)
    public static final int MAX_LEVEL_COUNT = (int) Math.log10(1000 * 1000 * 1000);
    private final ColumnFamilyStore cfs;
    @VisibleForTesting
    protected final List<SSTableReader>[] generations;
    private final List<SSTableReader>[] buffer;
    private final long maxSSTableSizeInBytes;
    private final SizeTieredCompactionStrategyOptions options;
    private final int [] compactionCounter;
    private final int levelFanoutSize;
    private final long firstTimeWindowSize;

    private final Pair<Long, Long>[] timeWindow;

    TimeLeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB, int fanoutSize, long firstTimeWindow, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024L * 1024L;
        this.options = options;
        this.levelFanoutSize = fanoutSize;

        generations = new List[MAX_LEVEL_COUNT];
        timeWindow = new Pair[MAX_LEVEL_COUNT];
        buffer = new List[MAX_LEVEL_COUNT];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
            buffer[i] = new ArrayList<>();
        }
        compactionCounter = new int[MAX_LEVEL_COUNT];
        firstTimeWindowSize = firstTimeWindow;

    }

    public static TimeLeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int fanoutSize, List<SSTableReader> sstables)
    {
        return create(cfs, maxSSTableSize, fanoutSize, sstables, new SizeTieredCompactionStrategyOptions());
    }

    public static TimeLeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, int fanoutSize, Iterable<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options)
    {
        TimeLeveledManifest manifest = new TimeLeveledManifest(cfs, maxSSTableSize, fanoutSize, Long.MAX_VALUE, options);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : sstables)
        {
            manifest.add(ssTableReader);
        }
        for (int i = 1; i < manifest.getAllLevelSize().length; i++)
        {
            manifest.repairOverlappingSSTables(i);
        }
        manifest.calculateLastCompactedKeys();
        return manifest;
    }

    /**
     * If we want to start compaction in level n, find the newest (by modification time) file in level n+1
     * and use its last token for last compacted key in level n;
     */
    public void calculateLastCompactedKeys()
    {
        for (int i = 0; i < generations.length - 1; i++)
        {
            // this level is empty
            if (generations[i + 1].isEmpty())
                continue;

            long maxModificationTime = Long.MIN_VALUE;
            for (SSTableReader ssTableReader : generations[i + 1])
            {
                long modificationTime = ssTableReader.getCreationTimeFor(Component.DATA);
                if (modificationTime >= maxModificationTime)
                {
                    maxModificationTime = modificationTime;
                }
            }

        }
    }

    public synchronized void add(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();

        assert level < generations.length : "Invalid level " + level + " out of " + (generations.length - 1);
        logDistribution();
        if (canAddSSTable(reader))
        {
            // adding the sstable does not cause overlap in the level
            logger.trace("Adding {} to L{}", reader, level);
            generations[level].add(reader);
        }
        else
        {
            // this can happen if:
            // * a compaction has promoted an overlapping sstable to the given level, or
            //   was also supposed to add an sstable at the given level.
            // * we are moving sstables from unrepaired to repaired and the sstable
            //   would cause overlap
            //
            // The add(..):ed sstable will be sent to level 0
            try
            {
                reader.descriptor.getMetadataSerializer().mutateLevel(reader.descriptor, 0);
                reader.reloadSSTableMetadata();
            }
            catch (IOException e)
            {
                logger.error("Could not change sstable level - adding it at level 0 anyway, we will find it at restart.", e);
            }
            if (!contains(reader))
            {
                generations[0].add(reader);
            }
            else
            {
                // An SSTable being added multiple times to this manifest indicates a programming error, but we don't
                // throw an AssertionError because this shouldn't break the compaction strategy. Instead we log it
                // together with a RuntimeException so the stack is print for troubleshooting if this ever happens.
                logger.warn("SSTable {} is already present on leveled manifest and should not be re-added.", reader, new RuntimeException());
            }
        }
    }

    private boolean contains(SSTableReader reader)
    {
        for (int i = 0; i < generations.length; i++)
        {
            if (generations[i].contains(reader))
                return true;
        }
        return false;
    }

    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isTraceEnabled())
            logger.trace("Replacing [{}]", toString(removed));

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = Integer.MAX_VALUE;

        for (SSTableReader sstable : removed)
        {
            int thisLevel = remove(sstable);
            minLevel = Math.min(minLevel, thisLevel);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isTraceEnabled())
            logger.trace("Adding [{}]", toString(added));

        for (SSTableReader ssTableReader : added)
            add(ssTableReader);
    }

    public synchronized void repairOverlappingSSTables(int level)
    {
        SSTableReader previous = null;
        Collections.sort(generations[level], SSTableReader.sstableComparator);
        List<SSTableReader> outOfOrderSSTables = new ArrayList<>();
        for (SSTableReader current : generations[level])
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
            {
                logger.warn("At level {}, {} [{}, {}] overlaps {} [{}, {}].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 or due to the fact that you have dropped sstables from another node into the data directory. " +
                            "Sending back to L0.  If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also have rows out-of-order within an sstable",
                            level, previous, previous.first, previous.last, current, current.first, current.last);
                outOfOrderSSTables.add(current);
            }
            else
            {
                previous = current;
            }
        }

        if (!outOfOrderSSTables.isEmpty())
        {
            for (SSTableReader sstable : outOfOrderSSTables)
                sendBackToL0(sstable);
        }
    }

    /**
     * Checks if adding the sstable creates an overlap in the level
     * @param sstable the sstable to add
     * @return true if it is safe to add the sstable in the level.
     */
    private boolean canAddSSTable(SSTableReader sstable)
    {
        int level = sstable.getSSTableLevel();
        if (level == 0)
            return true;

        List<SSTableReader> copyLevel = new ArrayList<>(generations[level]);
        copyLevel.add(sstable);
        Collections.sort(copyLevel, SSTableReader.sstableComparator);

        SSTableReader previous = null;
        for (SSTableReader current : copyLevel)
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
                return false;
            previous = current;
        }
        return true;
    }

    private synchronized void sendBackToL0(SSTableReader sstable)
    {
        remove(sstable);
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
            sstable.reloadSSTableMetadata();
            add(sstable);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not reload sstable meta data", e);
        }
    }

    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    public long maxBytesForLevel(int level, long maxSSTableSizeInBytes)
    {
        return maxBytesForLevel(level, levelFanoutSize, maxSSTableSizeInBytes);
    }

    public static long maxBytesForLevel(int level, int levelFanoutSize, long maxSSTableSizeInBytes)
    {
        if (level == 0)
            return 4L * maxSSTableSizeInBytes;
        double bytes = Math.pow(levelFanoutSize, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    public synchronized CompactionCandidate getCompactionCandidates()
    {
        // during bootstrap we only do size tiering in L0 to make sure
        // the streamed files can be placed in their original levels
        calculateTimeWindow();
        if (StorageService.instance.isBootstrapMode())
        {
            int level = getLevel0Candidates(timeWindow);
            if (level != 0)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Compaction candidates for L{} are {}", 0, toString(buffer[level]));
                return new CompactionCandidate(buffer[level], level, cfs.getCompactionStrategyManager().getMaxSSTableBytes());
            }
            return null;
        }
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of MAX_COMPACTING_L0 sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of MAX_COMPACTING_L0,
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // -- see the javadoc for MAX_COMPACTING_L0.
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.
        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            if (sstables.isEmpty())
                continue; // mostly this just avoids polluting the debug log with zero scores
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getTracker().getCompacting());
            getOutOfRangeSSTables(remaining, i);
        }

        int maxNum = 0;
        int chosenBuffer = 0;
        for (int i = 0; i < buffer.length;  i++) {
            if (buffer[i].size() > maxNum) {
                chosenBuffer = i;
            }
        }

        if (buffer[chosenBuffer].size() > 0)
        {
            Collection<SSTableReader> candidates = getCandidatesFor(chosenBuffer, timeWindow);
            if (!candidates.isEmpty())
            {
                int nextLevel = getTargetLevel(candidates, timeWindow);
                candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                if (logger.isTraceEnabled())
                    logger.trace("Compaction candidates for L{} are {}", chosenBuffer, toString(candidates));
                return new CompactionCandidate(candidates, nextLevel, cfs.getCompactionStrategyManager().getMaxSSTableBytes());
            }
            else
            {
                logger.trace("No compaction candidates for L{}", chosenBuffer);
            }
        }

        // Higher levels are happy, time for a standard, non-STCS L0 compaction
        if (getLevel(0).isEmpty())
            return null;
        Collection<SSTableReader> candidates = getCandidatesFor(0, timeWindow);
        if (candidates.isEmpty())
        {
            return null;
        }
        return new CompactionCandidate(candidates, getTargetLevel(candidates, timeWindow), maxSSTableSizeInBytes);
    }

    private void getOutOfRangeSSTables(Set<SSTableReader> ssTableReaders, int level) {
        long maxTimeStamp = timeWindow[level].right;
        long minTimeStamp = timeWindow[level].left;
        List<SSTableReader> addToBuffer = new ArrayList<>();
        for (SSTableReader ssTableReader: ssTableReaders) {
            if (ssTableReader.getMaxTimestamp() < minTimeStamp
                || ssTableReader.getMinTimestamp() > maxTimeStamp)
                addToBuffer.add(ssTableReader);
        }

        for (SSTableReader ssTableReader: addToBuffer) {
            for (int i = 0; i < timeWindow.length; i++) {
                if (ssTableReader.getMinTimestamp() >= timeWindow[i].left
                    && ssTableReader.getMaxTimestamp() <= timeWindow[i].right)
                    buffer[i].add(ssTableReader);
            }
        }
    }

    private int getLevel0Candidates(Pair<Long, Long>[] timewindow)
    {
        assert !getLevel(0).isEmpty();
        logger.trace("Choosing candidates for L{}", 0);

        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();

        Set<SSTableReader> compactingL0 = getCompacting(0);

        boolean inBuffer0 = true;
        for (SSTableReader ssTableReader: compactingL0) {
            for (int i = 0; i < timewindow.length; i++) {
                Pair<Long, Long> currentTimeWindow = timeWindow[i];
                if (ssTableReader.getMinTimestamp() >= currentTimeWindow.left
                    && ssTableReader.getMaxTimestamp() <= currentTimeWindow.right)
                {
                    buffer[i].add(ssTableReader);
                    inBuffer0 = false;
                    break;
                }
            }
            if (inBuffer0)
                buffer[0].add(ssTableReader);
        }


        Set<SSTableReader> candidates = new HashSet<>();

        int maxSize = 0;
        int choosenBuffer = 0;
        for (int i = 0; i < timewindow.length; i++) {
            if (buffer[i].size() > maxSize) {
                maxSize = buffer[i].size();
                choosenBuffer = i;
            }
        }

        candidates.addAll(buffer[choosenBuffer]);

        if (candidates.size() > 0)
        {
            Set<SSTableReader> overlapping = overlapping(candidates, getLevel(choosenBuffer));
            if (Sets.intersection(overlapping, compacting).size() > 0)
                return 0;
            if (!overlapping(candidates, compactingL0).isEmpty())
                return 0;
            buffer[choosenBuffer].addAll(overlapping);
            return choosenBuffer;
        }
        else
            return 0;
    }

    private long getOldestTimeInLevel(Set<SSTableReader> sstables) {
        long minTimeStamp = Long.MAX_VALUE;
        for (SSTableReader ssTableReader: sstables) {
            if (minTimeStamp > ssTableReader.getMinTimestamp())
                minTimeStamp = ssTableReader.getMinTimestamp();
        }
        return minTimeStamp;
    }

    private void calculateTimeWindow() {
        long right = System.currentTimeMillis() * 1000;
        long timeRange;
        this.timeWindow[0] = Pair.create(right, right);
        for (int i = 1; i < MAX_LEVEL_COUNT; i++) {
            timeRange = (long) Math.pow(levelFanoutSize, i - 1) * firstTimeWindowSize;
            this.timeWindow[i] = Pair.create(right - timeRange, right);
            right = right - timeRange;
        }
        this.timeWindow[MAX_LEVEL_COUNT - 1] = Pair.create(0l, timeWindow[MAX_LEVEL_COUNT - 1].right);
    }


    private List<SSTableReader> getSSTablesForTTCS(Collection<SSTableReader> sstables)
    {
        Iterable<SSTableReader> candidates = cfs.getTracker().getUncompacting(sstables);
        List<Pair<List<SSTableReader>, Pair<Long, Long>>> disOrderedBucket = TimeTieredCompactionStrategy.getBucketsInBuffer(TimeTieredCompactionStrategy.createSSTableAndTimeWindowPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates)));
        long maxSSTableNum = 0;
        Pair<List<SSTableReader>, Pair<Long, Long>> longestBucket = null;
        for (Pair<List<SSTableReader>, Pair<Long, Long>> bucketWithRange: disOrderedBucket)
        {
            if (bucketWithRange.left.size() >= maxSSTableNum)
            {
                longestBucket = bucketWithRange;
                maxSSTableNum = bucketWithRange.left.size();
            }
        }
        if (longestBucket != null)
            return longestBucket.left;
        return Collections.emptyList();
    }


    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.length - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isTraceEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.trace("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.length - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    PartitionPosition max = null;
                    PartitionPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    if (min == null || max == null || min.equals(max)) // single partition sstables - we cannot include a high level sstable.
                        return candidates;
                    Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
                    Range<PartitionPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : getLevel(i))
                    {
                        Range<PartitionPosition> r = new Range<PartitionPosition>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        if (i >= generations.length)
            throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (generations.length - 1));
        return getLevel(i).size();
    }

    public synchronized int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isTraceEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!getLevel(i).isEmpty())
                {
                    logger.trace("L{} contains {} SSTables ({}) in {}",
                                 i,
                                 getLevel(i).size(),
                                 FBUtilities.prettyPrintMemory(SSTableReader.getTotalBytes(getLevel(i))),
                                 this);
                }
            }
        }
    }

    @VisibleForTesting
    public synchronized int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations[level].remove(reader);
        return level;
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.first.getToken();
        Token last = sstable.last.getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.first.getToken()) <= 0 ? first : sstable.first.getToken();
            last = last.compareTo(sstable.last.getToken()) >= 0 ? last : sstable.last.getToken();
        }
        return overlapping(first, last, others);
    }

    private static Set<SSTableReader> overlappingWithBounds(SSTableReader sstable, Map<SSTableReader, Bounds<Token>> others)
    {
        return overlappingWithBounds(sstable.first.getToken(), sstable.last.getToken(), others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    @VisibleForTesting
    static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        return overlappingWithBounds(start, end, genBounds(sstables));
    }

    private static Set<SSTableReader> overlappingWithBounds(Token start, Token end, Map<SSTableReader, Bounds<Token>> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<>();
        Bounds<Token> promotedBounds = new Bounds<Token>(start, end);

        for (Map.Entry<SSTableReader, Bounds<Token>> pair : sstables.entrySet())
        {
            if (pair.getValue().intersects(promotedBounds))
                overlapped.add(pair.getKey());
        }
        return overlapped;
    }

    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };

    private static Map<SSTableReader, Bounds<Token>> genBounds(Iterable<SSTableReader> ssTableReaders)
    {
        Map<SSTableReader, Bounds<Token>> boundsMap = new HashMap<>();
        for (SSTableReader sstable : ssTableReaders)
        {
            boundsMap.put(sstable, new Bounds<Token>(sstable.first.getToken(), sstable.last.getToken()));
        }
        return boundsMap;
    }

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getCandidatesFor(int level, Pair<Long, Long>[] timewindow)
    {
        assert !getLevel(level).isEmpty();
        logger.trace("Choosing candidates for L{}", level);

        final Set<SSTableReader> compacting = cfs.getTracker().getCompacting();

        if (level == 0)
        {
            Set<SSTableReader> compactingL0 = getCompacting(0);

            PartitionPosition lastCompactingKey = null;
            PartitionPosition firstCompactingKey = null;
            for (SSTableReader candidate : compactingL0)
            {
                if (firstCompactingKey == null || candidate.first.compareTo(firstCompactingKey) < 0)
                    firstCompactingKey = candidate.first;
                if (lastCompactingKey == null || candidate.last.compareTo(lastCompactingKey) > 0)
                    lastCompactingKey = candidate.last;
            }

            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            Set<SSTableReader> candidates = new HashSet<>();
            Map<SSTableReader, Bounds<Token>> remaining = genBounds(Iterables.filter(getLevel(0), Predicates.not(suspectP)));

            for (SSTableReader sstable : ageSortedSSTables(remaining.keySet()))
            {
                if (candidates.contains(sstable))
                    continue;

                Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, remaining));
                if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                    continue;

                for (SSTableReader newCandidate : overlappedL0)
                {
                    if (firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Arrays.asList(newCandidate)).size() == 0)
                        candidates.add(newCandidate);
                    remaining.remove(newCandidate);
                }

                if (candidates.size() > MAX_COMPACTING_L0)
                {
                    // limit to only the MAX_COMPACTING_L0 oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_L0));
                    break;
                }
            }

            if (candidates.size() > 0)
            {
                int targetLevel = getTargetLevel(candidates, timewindow);
                Set<SSTableReader> overlapping = overlapping(candidates, getLevel(targetLevel));
                if (Sets.intersection(overlapping, compacting).size() > 0)
                    return Collections.emptyList();
                if (!overlapping(candidates, compactingL0).isEmpty())
                    return Collections.emptyList();
                candidates = Sets.union(candidates, overlapping);
                return candidates;
            }
            else
                return Collections.emptyList();

        }

        // for non-L0 compactions, pick up its buffer
        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        Map<SSTableReader, Bounds<Token>> sstablesNextLevel = genBounds(getLevel(level));
        for (int i = 0; i < buffer[level].size(); i++)
        {
            SSTableReader sstable = buffer[level].get(i);
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlappingWithBounds(sstable, sstablesNextLevel));
            if (Iterables.any(candidates, suspectP))
                continue;
            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    private Set<SSTableReader> getCompacting(int level)
    {
        Set<SSTableReader> sstables = new HashSet<>();
        Set<SSTableReader> levelSSTables = new HashSet<>(getLevel(level));
        for (SSTableReader sstable : cfs.getTracker().getCompacting())
        {
            if (levelSSTables.contains(sstable))
                sstables.add(sstable);
        }
        return sstables;
    }

    @VisibleForTesting
    List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<>(candidates);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampAscending);
        return ageSortedCandidates;
    }

    public synchronized Set<SSTableReader>[] getSStablesPerLevelSnapshot()
    {
        Set<SSTableReader>[] sstablesPerLevel = new Set[generations.length];
        for (int i = 0; i < generations.length; i++)
        {
            sstablesPerLevel[i] = new HashSet<>(generations[i]);
        }
        return sstablesPerLevel;
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (getLevel(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, getLevel(level));
    }

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            for (SSTableReader ssTableReader: sstables) {
                if (ssTableReader.getMaxTimestamp() <= timeWindow[i].left)
                    estimated[i]++;
            }
            tasks += estimated[i];
        }

        logger.trace("Estimating {} compactions to do for {}.{}",
                     Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }

    public int getTargetLevel(Collection<SSTableReader> sstables, Pair<Long, Long>[] timeWindow)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<>(sstables);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampAscending);

        long oldestTimeStamp = ageSortedCandidates.get(0).getMinTimestamp();
        int newLevel = 0;
        for (int i = 1; i < MAX_LEVEL_COUNT; i++) {
            if (oldestTimeStamp >= timeWindow[i].left)
            {
                newLevel = i;
                break;
            }
        }
        return newLevel;

    }

    public Iterable<SSTableReader> getAllSSTables()
    {
        Set<SSTableReader> sstables = new HashSet<>();
        for (List<SSTableReader> generation : generations)
        {
            sstables.addAll(generation);
        }
        return sstables;
    }

    public static class CompactionCandidate
    {
        public final Collection<SSTableReader> sstables;
        public final int level;
        public final long maxSSTableBytes;

        public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.level = level;
            this.maxSSTableBytes = maxSSTableBytes;
        }
    }
}
