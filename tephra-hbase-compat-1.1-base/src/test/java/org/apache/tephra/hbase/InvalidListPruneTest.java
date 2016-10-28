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

package org.apache.tephra.hbase;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionType;
import org.apache.tephra.TxConstants;
import org.apache.tephra.coprocessor.TransactionStateCache;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.apache.tephra.hbase.coprocessor.janitor.DataJanitorState;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.InMemoryTransactionStateStorage;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.persist.TransactionVisibilityState;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

/**
 * Test invalid list pruning
 */
public class InvalidListPruneTest extends AbstractHBaseTableTest {
  private static final byte[] family = Bytes.toBytes("f1");
  private static final byte[] qualifier = Bytes.toBytes("col1");

  private static TableName dataTable;
  private static TableName pruneStateTable;

  // Override AbstractHBaseTableTest.startMiniCluster to setup configuration
  @BeforeClass
  public static void startMiniCluster() throws Exception {
    // Setup the configuration to start HBase cluster with the invalid list pruning enabled
    conf = HBaseConfiguration.create();
    conf.setBoolean(TxConstants.DataJanitor.PRUNE_ENABLE, true);
    AbstractHBaseTableTest.startMiniCluster();

    TransactionStateStorage txStateStorage = new InMemoryTransactionStateStorage();
    TransactionManager txManager = new TransactionManager(conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();

    // Do some transactional data operations
    dataTable = TableName.valueOf("invalidListPruneTestTable");
    HTable hTable = createTable(dataTable.getName(), new byte[][]{family}, false,
                                Collections.singletonList(TestTransactionProcessor.class.getName()));
    try (TransactionAwareHTable txTable = new TransactionAwareHTable(hTable, TxConstants.ConflictDetection.ROW)) {
      TransactionContext txContext = new TransactionContext(new InMemoryTxSystemClient(txManager), txTable);
      txContext.start();
      for (int i = 0; i < 10; ++i) {
        txTable.put(new Put(Bytes.toBytes(i)).addColumn(family, qualifier, Bytes.toBytes(i)));
      }
      txContext.finish();
    }

    testUtil.flush(dataTable);
    txManager.stopAndWait();

    pruneStateTable = TableName.valueOf(conf.get(TxConstants.DataJanitor.PRUNE_STATE_TABLE,
                                                 TxConstants.DataJanitor.DEFAULT_PRUNE_STATE_TABLE));
  }

  @AfterClass
  public static void shutdownAfterClass() throws Exception {
    hBaseAdmin.disableTable(dataTable);
    hBaseAdmin.deleteTable(dataTable);
  }

  @Before
  public void beforeTest() throws Exception {
    HTable table = createTable(pruneStateTable.getName(), new byte[][]{DataJanitorState.FAMILY}, false,
                               // Prune state table is a non-transactional table, hence no transaction co-processor
                               Collections.<String>emptyList());
    table.close();
  }

  @After
  public void afterTest() throws Exception {
    hBaseAdmin.disableTable(pruneStateTable);
    hBaseAdmin.deleteTable(pruneStateTable);
  }

  @Test
  public void testRecordCompactionState() throws Exception {
    DataJanitorState dataJanitorState =
      new DataJanitorState(new DataJanitorState.TableSupplier() {
        @Override
        public Table get() throws IOException {
          return testUtil.getConnection().getTable(pruneStateTable);
        }
      });

    // No prune upper bound initially
    Assert.assertEquals(-1, dataJanitorState.getPruneUpperBound(getRegionName(dataTable, Bytes.toBytes(0))));

    // Create a new transaction snapshot
    InMemoryTransactionStateCache.setTransactionSnapshot(
      new TransactionSnapshot(100, 100, 100, ImmutableSet.of(50L),
                              ImmutableSortedMap.<Long, TransactionManager.InProgressTx>of()));
    // Run minor compaction
    testUtil.compact(dataTable, false);
    // No prune upper bound after minor compaction too
    Assert.assertEquals(-1, dataJanitorState.getPruneUpperBound(getRegionName(dataTable, Bytes.toBytes(0))));

    // Run major compaction, and verify prune upper bound
    testUtil.compact(dataTable, true);
    Assert.assertEquals(50, dataJanitorState.getPruneUpperBound(getRegionName(dataTable, Bytes.toBytes(0))));

    // Run major compaction again with same snapshot, prune upper bound should not change
    testUtil.compact(dataTable, true);
    Assert.assertEquals(50, dataJanitorState.getPruneUpperBound(getRegionName(dataTable, Bytes.toBytes(0))));

    // Create a new transaction snapshot
    InMemoryTransactionStateCache.setTransactionSnapshot(
      new TransactionSnapshot(110, 111, 112, ImmutableSet.of(150L),
                              ImmutableSortedMap.of(
                                105L, new TransactionManager.InProgressTx(100, 30, TransactionType.SHORT)
                              )
    ));
    Assert.assertEquals(50, dataJanitorState.getPruneUpperBound(getRegionName(dataTable, Bytes.toBytes(0))));

    // Run major compaction again, now prune upper bound should change
    testUtil.compact(dataTable, true);
    Assert.assertEquals(104, dataJanitorState.getPruneUpperBound(getRegionName(dataTable, Bytes.toBytes(0))));
  }

  private byte[] getRegionName(TableName dataTable, byte[] row) throws IOException {
    HRegionLocation regionLocation =
      testUtil.getConnection().getRegionLocator(dataTable).getRegionLocation(row);
    return regionLocation.getRegionInfo().getRegionName();
  }

  /**
   * A transaction co-processor that uses in-memory {@link TransactionSnapshot} for testing
   */
  @SuppressWarnings("WeakerAccess")
  public static class TestTransactionProcessor extends TransactionProcessor {
    @Override
    protected Supplier<TransactionStateCache> getTransactionStateCacheSupplier(RegionCoprocessorEnvironment env) {
      return new Supplier<TransactionStateCache>() {
        @Override
        public TransactionStateCache get() {
          return new InMemoryTransactionStateCache();
        }
      };
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
}
