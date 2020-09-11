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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.TrueTimeWindowCompactionStrategy;
import org.apache.cassandra.db.compaction.TrueTimeWindowCompactionStrategyOptions;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.UnfilteredRows;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
            List<RowIndexEntry> rieA = Lists.newArrayList();
            if (partition.hasNext()) {
                UnfilteredRows rows = (UnfilteredRows) partition;

                if (rows.metadata().clusteringColumns().size() > 0) {
                    RowIndexEntry rie = null;
                    ListMultimap<Long, Row> mRedisRows = ArrayListMultimap.create();
                    partition.forEachRemaining(unfiltered -> {
                        if (unfiltered.isRow()) {
                            Row irow = (Row) unfiltered;
                            long span = getSpan(irow, partition);
                            if (span > 0) {
                                mRedisRows.put(span, irow);
                            }
                        }
                    });
                    for (Long span: mRedisRows.keySet()) {
                        Collection<Row> srows = mRedisRows.get(span);
                        Iterator<Row> rowsIterator = srows.iterator();//Arrays.asList(srows).iterator();
                        UnfilteredRowIterator tmpRow =  new AbstractUnfilteredRowIterator(partition.metadata(), partition.partitionKey(),
                                DeletionTime.LIVE, partition.columns(), Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS) {
                            @Override
                            protected Unfiltered computeNext() {
                                return rowsIterator.hasNext() ? rowsIterator.next() : endOfData();
                            }
                        };
                        switchWriter(location, span);
                        rie = sstableWriter.append(tmpRow);
                        rieA.add(rie);
                        logger.trace("realAppend {} size {} - span {}", partition.toString(), srows.size(), span);
                    }

//                        RowIndexEntry rie = null;
//                        if (unfiltered.isRow()) {
//                            Row irow = (Row) unfiltered;
//                            UnfilteredRowIterator tmpRow = null;
//                            Iterator<Row> rowsIterator = Arrays.asList(irow).iterator();
////                            //tmpRow = Transformation.apply(rowsIterator, new BigTableWriter.StatsCollector(null));
//                            tmpRow =  new AbstractUnfilteredRowIterator(partition.metadata(), partition.partitionKey(),
//                                    DeletionTime.LIVE, partition.columns(), Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS) {
//                                private boolean returned;
//
//                                @Override
//                                protected Unfiltered computeNext() {
//                                    if (returned)
//                                        return endOfData();
//                                    returned = true;
//                                    return rowsIterator.next();
//                                }
//                            };
//                            long span = getSpan(irow, partition);
//
//                            if (span > 0) {
//                                switchWriter(location, span);
//                                rie = sstableWriter.append(tmpRow);
//                                rieA.add(rie);
//                                logger.trace("realAppend {} - span {}", partition, span);
//
//                                if(partition.hasNext())
//                                    switchCompactionLocation(location);
//
////                                if (preSpan != span) {
////                                    preSpan = span;
////                                }
//                            }
//                        }
//                    });
                } else {
                    Row row = (Row)rows.peek();
                    long span = getSpan(row, partition);
                    if (span > 0) {
                        switchWriter(location, span);
                        RowIndexEntry rie = sstableWriter.append(partition);
                        rieA.add(rie);
                        logger.trace("realAppend {} - span {}", partition, span);

                        if (preSpan != span) {
                            preSpan = span;
                        }
                    }
                }
            }
            return rieA.size() > 0;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("realAppend failed {}", e.toString());
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

    public boolean realAppend4WideRow(UnfilteredRowIterator partition)
    {
        try {
//        UnfilteredRowIterator it = new UnfilteredRowIterator(partition);
            List<RowIndexEntry> rieA = Lists.newArrayList();
            partition.forEachRemaining(unfiltered -> {
                if (unfiltered.isRow())
                {
                    Row row = (Row) unfiltered;
                    UnfilteredRowIterator tmpRow = partition;
                    if (row.clustering().size() > 0) {
                        Iterator<Row> rowsIterator = Arrays.asList(row).iterator();
                        tmpRow = new AbstractUnfilteredRowIterator(partition.metadata(), partition.partitionKey(),
                                DeletionTime.LIVE, partition.columns(), Rows.EMPTY_STATIC_ROW, false, EncodingStats.NO_STATS) {
                            @Override
                            protected Unfiltered computeNext() {
                                return rowsIterator.hasNext() ? rowsIterator.next() : endOfData();
                            }
                        };
                    }
                    long span = getSpan(row, partition);

                    if (span > 0) {
                        switchWriter(location, span);
                        RowIndexEntry rie = sstableWriter.append(tmpRow);
                        rieA.add(rie);
                        logger.trace("realAppend {} - span {}", partition, span);

                        if (preSpan != span) {
                            preSpan = span;
                        }
                    }
                }
            });
            return rieA.size() > 0;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("realAppend failed {}", e.toString());
            return false;
        }
    }

    private long getSpan(Row row, UnfilteredRowIterator partition) {
        long span = 0;
        long rowTimestamp = row.primaryKeyLivenessInfo().timestamp();

        long origRowTS = rowTimestamp;
        // get max timestamp, a  row' timestamp can get from the row-livenessinfo or cells,
        // or from both.
        //if (rowTimestamp == Long.MIN_VALUE) {
        Iterable<Cell> cells = row.cells();
        for (Cell cell: cells) {
            if(cell.timestamp() > rowTimestamp) {
                rowTimestamp = cell.timestamp();
                //break;
            }
        }
        if (origRowTS < 0) {
            origRowTS = rowTimestamp;
        } else if (rowTimestamp < 0) {
            rowTimestamp = origRowTS;
        }

        long otStamp = 0, tStamp = 0;
        Pair<Long, Long> boundsMax = null, boundsOrig = null;
        long minSplitStamp = System.currentTimeMillis() - options.minSStableSplitWindow * options.timeWindowInMillis;
        if (origRowTS != rowTimestamp) {
            otStamp = TimeUnit.MILLISECONDS.convert(origRowTS, TimeUnit.MICROSECONDS);
            tStamp = TimeUnit.MILLISECONDS.convert(rowTimestamp, TimeUnit.MICROSECONDS);
            if (tStamp < minSplitStamp && otStamp < minSplitStamp) {
                boundsOrig = boundsMax = TrueTimeWindowCompactionStrategy.getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, minSplitStamp);
            } else {
                boundsMax = TrueTimeWindowCompactionStrategy.getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, tStamp);
                boundsOrig = TrueTimeWindowCompactionStrategy.getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, otStamp);
            }
            if (boundsOrig.left != boundsMax.left) {
                //rows.next();
                logger.error("realAppend failed, drop record {}/{}-{} ", partition.partitionKey(), otStamp, tStamp);
                return -1; // cannot decide which partition this row belongs to, ignore it.
            }
        } else {
            tStamp = TimeUnit.MILLISECONDS.convert(rowTimestamp, TimeUnit.MICROSECONDS);
            if (tStamp < minSplitStamp) {
                tStamp = minSplitStamp;
            }
            boundsMax = TrueTimeWindowCompactionStrategy.getWindowBoundsInMillis(options.sstableWindowUnit, options.sstableWindowSize, tStamp);
        }

        span = (long) (boundsMax.left / options.timeWindowInMillis);
        logger.trace("xxxxxxx  row {}/{} ts {} - span {} / boundsMax.left {}", partition.partitionKey(), row.clustering().getRawValues(),
                rowTimestamp, span, boundsMax.left);
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
        //sstableWriter.switchWriter(writer);

        logger.debug("Switching to new writer {} for span {}", writer.descriptor.baseFilename(), span);
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
        logger.debug("switchCompactionLocation create new writer {}", writer.descriptor.baseFilename());
        sstableWriter.setCurrentWriter(writer);
        locationSwitched = true;
    }
}
