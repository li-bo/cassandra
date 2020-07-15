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
package org.apache.cassandra.db.compaction.writers;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.TrueTimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.TrueTimeWindowCompactionStrategyOptions;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.UnfilteredRows;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * CompactionAwareWriter that splits input in differently sized sstables
 *
 * Biggest sstable will be total_compaction_size / 2, second biggest total_compaction_size / 4 etc until
 * the result would be sub 50MB, all those are put in the same
 */
public class SplittingTimeWindowCompactionWriter extends CompactionAwareWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SplittingSizeTieredCompactionWriter.class);

    private final long totalSize;
    private final Set<SSTableReader> allSSTables;
    private long expectedBloomFilterSize = 0;
    private Directories.DataDirectory location;
    private HashMap<Long, SSTableWriter> writerHashMap = new HashMap<>();
    private TrueTimeWindowCompactionStrategyOptions options;
    boolean locationSwitched = false;
    private long preSpan = -1;

    public SplittingTimeWindowCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn,
                                               Set<SSTableReader> nonExpiredSSTables, TrueTimeWindowCompactionStrategyOptions options)
    {
        this(cfs, directories, txn, nonExpiredSSTables);
        this.options = options;
    }

    public SplittingTimeWindowCompactionWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn,
                                               Set<SSTableReader> nonExpiredSSTables)
    {
        super(cfs, directories, txn, nonExpiredSSTables, false, false);
        this.allSSTables = txn.originals();
        expectedBloomFilterSize = Math.max(cfs.metadata.params.minIndexInterval, (int)(SSTableReader.getApproximateKeyCount(nonExpiredSSTables)));

        totalSize = cfs.getExpectedCompactedFileSize(nonExpiredSSTables, txn.opType());
    }

    @Override
    public boolean realAppend(UnfilteredRowIterator partition)
    {
        try {
//        UnfilteredRowIterator it = new UnfilteredRowIterator(partition);
            long span = getSpan(partition);

            switchWriter(location, span);
            RowIndexEntry rie = sstableWriter.append(partition);
            logger.trace("realAppend {} - span {}", partition, span);

            if (preSpan != span) {
                preSpan = span;
            }
            return rie != null;
        } catch (Exception e) {
            logger.error("realAppend failed {}", e.getMessage());
            return false;
        }
    }

    private long getSpan(UnfilteredRowIterator partition) {
        long span = 0;

       if (partition.hasNext())
        {
            UnfilteredRows rows = (UnfilteredRows) partition;
            Row row = (Row)rows.peek();
            Long rowTimestamp = row.primaryKeyLivenessInfo().timestamp();
            logger.trace("xxxxxxx  row info {}", rowTimestamp);
            long tStamp = TimeUnit.MILLISECONDS.convert(rowTimestamp, TimeUnit.MICROSECONDS);
            long minSplitStamp = System.currentTimeMillis() - options.minSStableSplitWindow * options.timeWindowInMillis;
            if (tStamp < minSplitStamp) {
                tStamp = minSplitStamp;
            }
            Pair<Long, Long> boundsMax = TrueTimeWindowCompactionStrategy.getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, tStamp);
            span = (long) (boundsMax.left / options.timeWindowInMillis);
        }
        return span;
    }

    private void switchWriter(Directories.DataDirectory location, long span) {
        if (span == preSpan) return;
        if (locationSwitched && sstableWriter.currentWriter() != null) {
            writerHashMap.put(span, sstableWriter.currentWriter());
            locationSwitched = false;
            logger.trace("Switching to pre-created writer {} for span {}",
                    writerHashMap.get(span).descriptor.baseFilename(), span);
            return;
        }
        if (writerHashMap.get(span) != null) {
            sstableWriter.setCurrentWriter(writerHashMap.get(span));
            logger.trace("Switching to existed writer {} for span {}",
                    writerHashMap.get(span).descriptor.baseFilename(), span);
            return;
        }

        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(location))),
                estimatedTotalKeys,
                minRepairedAt,
                cfs.metadata,
                new MetadataCollector(allSSTables, cfs.metadata.comparator, 0),
                SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                cfs.indexManager.listIndexes(),
                txn);
        writerHashMap.put(span, writer);
        sstableWriter.setCurrentWriter(writer);
        logger.trace("Switching to new writer {} for span {}", writer.descriptor.baseFilename(), span);
    }

    @Override
    public void switchCompactionLocation(Directories.DataDirectory location)
    {
        this.location = location;
        @SuppressWarnings("resource")
        SSTableWriter writer = SSTableWriter.create(Descriptor.fromFilename(cfs.getSSTablePath(getDirectories().getLocationForDisk(location))),
                                                    expectedBloomFilterSize,
                                                    minRepairedAt,
                                                    cfs.metadata,
                                                    new MetadataCollector(allSSTables, cfs.metadata.comparator, 0),
                                                    SerializationHeader.make(cfs.metadata, nonExpiredSSTables),
                                                    cfs.indexManager.listIndexes(),
                                                    txn);
        logger.trace("switchCompactionLocation create new writer {}", writer.descriptor.baseFilename());
        sstableWriter.setCurrentWriter(writer);
        locationSwitched = true;
    }
}
