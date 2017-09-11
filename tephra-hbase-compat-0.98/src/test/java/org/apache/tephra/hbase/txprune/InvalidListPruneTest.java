/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tephra.hbase.txprune;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.CacheSupplier;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.hbase.AbstractHBaseTableTest;
import org.apache.tephra.hbase.TransactionAwareHTable;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.InMemoryTransactionStateStorage;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.persist.TransactionVisibilityState;
import org.apache.tephra.txprune.TransactionPruningPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test invalid list pruning
 */
public class InvalidListPruneTest extends AbstractHBaseTableTest {
  private static final byte[] family = Bytes.toBytes("f1");
  private static final byte[] qualifier = Bytes.toBytes("col1");
  private static final int MAX_ROWS = 1000;

  private static TableName txDataTable1;
  private static TableName pruneStateTable;
  private static DataJanitorState dataJanitorState;

  private static HConnection connection;

  // Override AbstractHBaseTableTest.startMiniCluster to setup configuration
  @BeforeClass
  public static void startMiniCluster() throws Exception {
    // Setup the configuration to start HBase cluster with the invalid list pruning enabled
    conf = HBaseConfiguration.create();
    conf.setBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE, true);
    // Flush prune data to table quickly, so that tests don't need have to wait long to see updates
    conf.setLong(TxConstants.TransactionPruning.PRUNE_FLUSH_INTERVAL, 0L);
    AbstractHBaseTableTest.startMiniCluster();

    TransactionStateStorage txStateStorage = new InMemoryTransactionStateStorage();
    TransactionManager txManager = new TransactionManager(conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();

    // Do some transactional data operations
    txDataTable1 = TableName.valueOf("invalidListPruneTestTable1");
    HTable hTable = createTable(txDataTable1.getName(), new byte[][]{family}, false,
                                Collections.singletonList(TestTransactionProcessor.class.getName()));
    try (TransactionAwareHTable txTable = new TransactionAwareHTable(hTable, TxConstants.ConflictDetection.ROW)) {
      TransactionContext txContext = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable);
      txContext.start();
      for (int i = 0; i < MAX_ROWS; ++i) {
        txTable.put(new Put(Bytes.toBytes(i)).add(family, qualifier, Bytes.toBytes(i)));
      }
      txContext.finish();
    }

    testUtil.flush(txDataTable1);
    txManager.stopAndWait();

    pruneStateTable = TableName.valueOf(conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                                 TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE));
    connection = HConnectionManager.createConnection(conf);
    dataJanitorState =
      new DataJanitorState(new DataJanitorState.TableSupplier() {
        @Override
        public HTableInterface get() throws IOException {
          return connection.getTable(pruneStateTable);
        }
      });
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    connection.close();
    hBaseAdmin.disableTable(txDataTable1);
    hBaseAdmin.deleteTable(txDataTable1);
  }

  @Before
  public void beforeTest() throws Exception {
    createPruneStateTable();
    InMemoryTransactionStateCache.setTransactionSnapshot(null);
  }

  private void createPruneStateTable() throws Exception {
    HTable table = createTable(pruneStateTable.getName(), new byte[][]{DataJanitorState.FAMILY}, false,
                               // Prune state table is a non-transactional table, hence no transaction co-processor
                               Collections.<String>emptyList());
    table.close();
  }

  @After
  public void afterTest() throws Exception {
    // Disable the data table so that prune writer thread gets stopped,
    // this makes sure that any cached value will not interfere with next test
    hBaseAdmin.disableTable(txDataTable1);
    deletePruneStateTable();
    // Enabling the table enables the prune writer thread again
    hBaseAdmin.enableTable(txDataTable1);
  }

  private void deletePruneStateTable() throws Exception {
    if (hBaseAdmin.tableExists(pruneStateTable)) {
      hBaseAdmin.disableTable(pruneStateTable);
      hBaseAdmin.deleteTable(pruneStateTable);
    }
  }

  @Test
  public void testRecordCompactionState() throws Exception {
    // No prune upper bound initially
    Assert.assertEquals(-1,
                        dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));

    // Create a new transaction snapshot
    InMemoryTransactionStateCache.setTransactionSnapshot(
      new TransactionSnapshot(100, 100, 100, ImmutableSet.of(50L),
                              ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));
    // Run minor compaction
    testUtil.compact(txDataTable1, false);
    // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
    TimeUnit.SECONDS.sleep(2);
    // No prune upper bound after minor compaction too
    Assert.assertEquals(-1,
                        dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));

    // Run major compaction, and verify prune upper bound
    testUtil.compact(txDataTable1, true);
    // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(50,
                        dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));

    // Run major compaction again with same snapshot, prune upper bound should not change
    testUtil.compact(txDataTable1, true);
    // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(50,
                        dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));

    // Create a new transaction snapshot
    InMemoryTransactionStateCache.setTransactionSnapshot(
      new TransactionSnapshot(110, 111, 112, ImmutableSet.of(150L),
                              ImmutableSortedMap.of(105L, new TransactionManager.InProgressTx(
                                100, 30, TransactionManager.InProgressType.SHORT))));
    Assert.assertEquals(50,
                        dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));

    // Run major compaction again, now prune upper bound should change
    testUtil.compact(txDataTable1, true);
    // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
    TimeUnit.SECONDS.sleep(2);
    Assert.assertEquals(104,
                        dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));
  }

  @Test
  public void testRecordCompactionStateNoTable() throws Exception {
    // Create a new transaction snapshot
    InMemoryTransactionStateCache.setTransactionSnapshot(
      new TransactionSnapshot(100, 100, 100, ImmutableSet.of(50L),
                              ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));
    // Run major compaction, and verify it completes
    long now = System.currentTimeMillis();
    testUtil.compact(txDataTable1, true);
    // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
    TimeUnit.SECONDS.sleep(2);
    long lastMajorCompactionTime = TestTransactionProcessor.lastMajorCompactionTime.get();
    Assert.assertTrue(String.format("Expected %d, but was %d", now, lastMajorCompactionTime),
                      lastMajorCompactionTime >= now);
  }

  @Test
  public void testRecordCompactionStateNoTxSnapshot() throws Exception {
    // Test recording state without having a transaction snapshot to make sure we don't disrupt
    // major compaction in that case
    InMemoryTransactionStateCache.setTransactionSnapshot(null);
    // Run major compaction, and verify it completes
    long now = System.currentTimeMillis();
    testUtil.compact(txDataTable1, true);
    // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
    TimeUnit.SECONDS.sleep(2);
    long lastMajorCompactionTime = TestTransactionProcessor.lastMajorCompactionTime.get();
    Assert.assertTrue(String.format("Expected %d, but was %d", now, lastMajorCompactionTime),
                      lastMajorCompactionTime >= now);
  }

  @Test
  public void testPruneUpperBound() throws Exception {
    TransactionPruningPlugin transactionPruningPlugin = new TestTransactionPruningPlugin();
    transactionPruningPlugin.initialize(conf);

    try {
      // Run without a transaction snapshot first
      long now1 = 200;
      long inactiveTxTimeNow1 = 150 * TxConstants.MAX_TX_PER_MS;
      long expectedPruneUpperBound1 = -1;
      // fetch prune upper bound
      long pruneUpperBound1 = transactionPruningPlugin.fetchPruneUpperBound(now1, inactiveTxTimeNow1);
      Assert.assertEquals(expectedPruneUpperBound1, pruneUpperBound1);

      TimeRegions expectedRegions1 =
        new TimeRegions(now1,
                        ImmutableSortedSet.orderedBy(Bytes.BYTES_COMPARATOR)
                          .add(getRegionName(txDataTable1, Bytes.toBytes(0)))
                          .build());
      // Assert prune state is recorded correctly
      Assert.assertEquals(expectedRegions1, dataJanitorState.getRegionsOnOrBeforeTime(now1));
      Assert.assertEquals(expectedPruneUpperBound1,
                          dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));
      Assert.assertEquals(inactiveTxTimeNow1, dataJanitorState.getInactiveTransactionBoundForTime(now1));

      // Run prune complete
      transactionPruningPlugin.pruneComplete(now1, expectedPruneUpperBound1);

      // Assert prune state was cleaned up correctly based on the prune time
      Assert.assertEquals(expectedRegions1, dataJanitorState.getRegionsOnOrBeforeTime(now1));
      Assert.assertEquals(expectedPruneUpperBound1,
                          dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));
      Assert.assertEquals(inactiveTxTimeNow1, dataJanitorState.getInactiveTransactionBoundForTime(now1));

      // Create a new transaction snapshot, and run major compaction on txDataTable1
      // And run all assertions again
      long now2 = 300;
      long inactiveTxTimeNow2 = 250 * TxConstants.MAX_TX_PER_MS;
      long expectedPruneUpperBound2 = 200 * TxConstants.MAX_TX_PER_MS;
      InMemoryTransactionStateCache.setTransactionSnapshot(
        new TransactionSnapshot(expectedPruneUpperBound2, expectedPruneUpperBound2, expectedPruneUpperBound2,
                                ImmutableSet.of(expectedPruneUpperBound2),
                                ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));
      TimeRegions expectedRegions2 =
        new TimeRegions(now2,
                        ImmutableSortedSet.orderedBy(Bytes.BYTES_COMPARATOR)
                          .add(getRegionName(txDataTable1, Bytes.toBytes(0)))
                          .build());
      testUtil.compact(txDataTable1, true);
      // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
      TimeUnit.SECONDS.sleep(2);
      long pruneUpperBound2 = transactionPruningPlugin.fetchPruneUpperBound(now2, inactiveTxTimeNow2);
      Assert.assertEquals(expectedPruneUpperBound2, pruneUpperBound2);

      Assert.assertEquals(expectedRegions2, dataJanitorState.getRegionsOnOrBeforeTime(now2));
      Assert.assertEquals(expectedPruneUpperBound2,
                          dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));
      Assert.assertEquals(inactiveTxTimeNow2, dataJanitorState.getInactiveTransactionBoundForTime(now2));
      Assert.assertEquals(expectedRegions1, dataJanitorState.getRegionsOnOrBeforeTime(now1));
      Assert.assertEquals(inactiveTxTimeNow1, dataJanitorState.getInactiveTransactionBoundForTime(now1));

      transactionPruningPlugin.pruneComplete(now2, pruneUpperBound2);
      Assert.assertEquals(expectedRegions2, dataJanitorState.getRegionsOnOrBeforeTime(now2));
      Assert.assertEquals(expectedPruneUpperBound2,
                          dataJanitorState.getPruneUpperBoundForRegion(getRegionName(txDataTable1, Bytes.toBytes(0))));
      Assert.assertEquals(inactiveTxTimeNow2, dataJanitorState.getInactiveTransactionBoundForTime(now2));
      Assert.assertNull(dataJanitorState.getRegionsOnOrBeforeTime(now1));
      Assert.assertEquals(expectedPruneUpperBound1, dataJanitorState.getInactiveTransactionBoundForTime(now1));

    } finally {
      transactionPruningPlugin.destroy();
    }
  }

  @Test
  public void testPruneEmptyTable() throws Exception {
    // Make sure that empty tables do not block the progress of pruning

    // Create an empty table
    TableName txEmptyTable = TableName.valueOf("emptyPruneTestTable");
    HTable emptyHTable = createTable(txEmptyTable.getName(), new byte[][]{family}, false,
                                     Collections.singletonList(TestTransactionProcessor.class.getName()));

    TransactionPruningPlugin transactionPruningPlugin = new TestTransactionPruningPlugin();
    transactionPruningPlugin.initialize(conf);

    try {
      long now1 = System.currentTimeMillis();
      long inactiveTxTimeNow1 = (now1 - 150) * TxConstants.MAX_TX_PER_MS;
      long noPruneUpperBound = -1;
      long expectedPruneUpperBound1 = (now1 - 200) * TxConstants.MAX_TX_PER_MS;
      InMemoryTransactionStateCache.setTransactionSnapshot(
        new TransactionSnapshot(expectedPruneUpperBound1, expectedPruneUpperBound1, expectedPruneUpperBound1,
                                ImmutableSet.of(expectedPruneUpperBound1),
                                ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));
      testUtil.compact(txEmptyTable, true);
      testUtil.compact(txDataTable1, true);
      // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
      TimeUnit.SECONDS.sleep(2);

      // fetch prune upper bound, there should be no prune upper bound since txEmptyTable cannot be compacted
      long pruneUpperBound1 = transactionPruningPlugin.fetchPruneUpperBound(now1, inactiveTxTimeNow1);
      Assert.assertEquals(noPruneUpperBound, pruneUpperBound1);
      transactionPruningPlugin.pruneComplete(now1, noPruneUpperBound);

      // Now flush the empty table, this will record the table region as empty, and then pruning will continue
      testUtil.flush(txEmptyTable);
      // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
      TimeUnit.SECONDS.sleep(2);

      // fetch prune upper bound, again, this time it should work
      pruneUpperBound1 = transactionPruningPlugin.fetchPruneUpperBound(now1, inactiveTxTimeNow1);
      Assert.assertEquals(expectedPruneUpperBound1, pruneUpperBound1);
      transactionPruningPlugin.pruneComplete(now1, expectedPruneUpperBound1);

      // Now add some data to the empty table
      // (adding data non-transactionally is okay too, we just need some data for the compaction to run)
      emptyHTable.put(new Put(Bytes.toBytes(1)).add(family, qualifier, Bytes.toBytes(1)));
      emptyHTable.close();

      // Now run another compaction on txDataTable1 with an updated tx snapshot
      long now2 = System.currentTimeMillis();
      long inactiveTxTimeNow2 = (now2 - 150) * TxConstants.MAX_TX_PER_MS;
      long expectedPruneUpperBound2 = (now2 - 200) * TxConstants.MAX_TX_PER_MS;
      InMemoryTransactionStateCache.setTransactionSnapshot(
        new TransactionSnapshot(expectedPruneUpperBound2, expectedPruneUpperBound2, expectedPruneUpperBound2,
                                ImmutableSet.of(expectedPruneUpperBound2),
                                ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));
      testUtil.flush(txEmptyTable);
      testUtil.compact(txDataTable1, true);
      // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
      TimeUnit.SECONDS.sleep(2);

      // Running a prune now should still return min(inactiveTxTimeNow1, expectedPruneUpperBound1) since
      // txEmptyTable is no longer empty. This information is returned since the txEmptyTable was recorded as being
      // empty in the previous run with inactiveTxTimeNow1
      long pruneUpperBound2 = transactionPruningPlugin.fetchPruneUpperBound(now2, inactiveTxTimeNow2);
      Assert.assertEquals(inactiveTxTimeNow1, pruneUpperBound2);
      transactionPruningPlugin.pruneComplete(now2, expectedPruneUpperBound1);

      // However, after compacting txEmptyTable we should get the latest upper bound
      testUtil.flush(txEmptyTable);
      testUtil.compact(txEmptyTable, true);
      // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
      TimeUnit.SECONDS.sleep(2);
      pruneUpperBound2 = transactionPruningPlugin.fetchPruneUpperBound(now2, inactiveTxTimeNow2);
      Assert.assertEquals(expectedPruneUpperBound2, pruneUpperBound2);
      transactionPruningPlugin.pruneComplete(now2, expectedPruneUpperBound2);
    } finally {
      transactionPruningPlugin.destroy();
      hBaseAdmin.disableTable(txEmptyTable);
      hBaseAdmin.deleteTable(txEmptyTable);
    }
  }

  @Test
  public void testPruneTransientTable() throws Exception {
    // Make sure that transient tables do not block the progress of pruning

    // Create a temp table
    TableName txTempTable = TableName.valueOf("tempTable");
    createTable(txTempTable.getName(), new byte[][]{family}, false,
                Collections.singletonList(TestTransactionProcessor.class.getName()));

    TableName txDataTable2 = null;

    TransactionPruningPlugin transactionPruningPlugin = new TestTransactionPruningPlugin();
    transactionPruningPlugin.initialize(conf);

    try {
      long now1 = System.currentTimeMillis();
      long inactiveTxTimeNow1 = (now1 - 150) * TxConstants.MAX_TX_PER_MS;
      long noPruneUpperBound = -1;
      long expectedPruneUpperBound1 = (now1 - 200) * TxConstants.MAX_TX_PER_MS;
      InMemoryTransactionStateCache.setTransactionSnapshot(
        new TransactionSnapshot(expectedPruneUpperBound1, expectedPruneUpperBound1, expectedPruneUpperBound1,
                                ImmutableSet.of(expectedPruneUpperBound1),
                                ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));

      // fetch prune upper bound, there should be no prune upper bound since nothing has been compacted yet.
      // This run is only to store the initial set of regions
      long pruneUpperBound1 = transactionPruningPlugin.fetchPruneUpperBound(now1, inactiveTxTimeNow1);
      Assert.assertEquals(noPruneUpperBound, pruneUpperBound1);
      transactionPruningPlugin.pruneComplete(now1, noPruneUpperBound);

      // Now delete the transient table
      hBaseAdmin.disableTable(txTempTable);
      hBaseAdmin.deleteTable(txTempTable);
      txTempTable = null;

      // Compact the data table now
      testUtil.compact(txDataTable1, true);
      // Since the write to prune table happens async, we need to sleep a bit before checking the state of the table
      TimeUnit.SECONDS.sleep(2);

      // Create a new table that will not be compacted
      txDataTable2 = TableName.valueOf("invalidListPruneTestTable2");
      createTable(txDataTable2.getName(), new byte[][]{family}, false,
                  Collections.singletonList(TestTransactionProcessor.class.getName()));

      // fetch prune upper bound, there should be a prune upper bound even though txTempTable does not exist anymore,
      // and txDataTable2 has not been compacted/flushed yet
      long now2 = System.currentTimeMillis();
      long inactiveTxTimeNow2 = (now1 - 150) * TxConstants.MAX_TX_PER_MS;
      long pruneUpperBound2 = transactionPruningPlugin.fetchPruneUpperBound(now2, inactiveTxTimeNow2);
      Assert.assertEquals(expectedPruneUpperBound1, pruneUpperBound2);
      transactionPruningPlugin.pruneComplete(now2, expectedPruneUpperBound1);
    } finally {
      transactionPruningPlugin.destroy();
      if (txDataTable2 != null) {
        hBaseAdmin.disableTable(txDataTable2);
        hBaseAdmin.deleteTable(txDataTable2);
      }
      if (txTempTable != null) {
        hBaseAdmin.disableTable(txTempTable);
        hBaseAdmin.deleteTable(txTempTable);
      }
    }
  }

  private byte[] getRegionName(TableName dataTable, byte[] row) throws IOException {
    HRegionLocation regionLocation = connection.getRegionLocation(dataTable, row, true);
    return regionLocation.getRegionInfo().getRegionName();
  }

  /**
   * A transaction co-processor that uses in-memory {@link TransactionSnapshot} for testing
   */
  @SuppressWarnings("WeakerAccess")
  public static class TestTransactionProcessor extends TransactionProcessor {
    private static final AtomicLong lastMajorCompactionTime = new AtomicLong(-1);

    @Override
    protected CacheSupplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
      return new CacheSupplier<TransactionStateCache>() {
        @Override
        public TransactionStateCache get() {
          return new InMemoryTransactionStateCache();
        }

        @Override
        public void release() {
          // no-op
        }
      };
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, StoreFile resultFile,
                            CompactionRequest request) throws IOException {
      super.postCompact(e, store, resultFile, request);
      lastMajorCompactionTime.set(System.currentTimeMillis());
    }
  }

  /**
   * Used to supply in-memory {@link TransactionSnapshot} to {@link TestTransactionProcessor} for testing
   */
  @SuppressWarnings("WeakerAccess")
  public static class InMemoryTransactionStateCache extends TransactionStateCache {
    private static TransactionVisibilityState transactionSnapshot;

    public static void setTransactionSnapshot(TransactionVisibilityState transactionSnapshot) {
      InMemoryTransactionStateCache.transactionSnapshot = transactionSnapshot;
    }

    @Override
    protected void startUp() throws Exception {
      // Nothing to do
    }

    @Override
    protected void shutDown() throws Exception {
      // Nothing to do
    }

    @Override
    public TransactionVisibilityState getLatestState() {
      return transactionSnapshot;
    }
  }

  @SuppressWarnings("WeakerAccess")
  public static class TestTransactionPruningPlugin extends HBaseTransactionPruningPlugin {
    @Override
    protected boolean isTransactionalTable(HTableDescriptor tableDescriptor) {
      return tableDescriptor.hasCoprocessor(TestTransactionProcessor.class.getName());
    }
  }
}
