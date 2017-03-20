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

package org.apache.tephra.txprune;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TxConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test {@link TransactionPruningService}.
 */
public class TransactionPruningServiceTest {

  @Before
  public void resetData() {
    MockTxManager.getPrunedInvalidsList().clear();

    MockPlugin1.getInactiveTransactionBoundList().clear();
    MockPlugin1.getMaxPrunedInvalidList().clear();

    MockPlugin2.getInactiveTransactionBoundList().clear();
    MockPlugin2.getMaxPrunedInvalidList().clear();
  }

  @Test
  public void testTransactionPruningService() throws Exception {
    // Setup plugins
    Configuration conf = new Configuration();
    conf.set(TxConstants.TransactionPruning.PLUGINS,
             "data.tx.txprune.plugin.mockPlugin1, data.tx.txprune.plugin.mockPlugin2");
    conf.set("data.tx.txprune.plugin.mockPlugin1.class",
             "org.apache.tephra.txprune.TransactionPruningServiceTest$MockPlugin1");
    conf.set("data.tx.txprune.plugin.mockPlugin2.class",
             "org.apache.tephra.txprune.TransactionPruningServiceTest$MockPlugin2");
    // Setup schedule to run every second
    conf.setBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE, true);
    conf.setInt(TxConstants.TransactionPruning.PRUNE_INTERVAL, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10);
    conf.setLong(TxConstants.TransactionPruning.PRUNE_GRACE_PERIOD, 0);

    // Setup mock data
    long m = 1000;
    long n = m * TxConstants.MAX_TX_PER_MS;
    // Current time to be returned
    Iterator<Long> currentTime = Iterators.cycle(120L * m, 220L * m);
    // Transaction objects to be returned by mock tx manager
    Iterator<Transaction> txns =
      Iterators.cycle(new Transaction(100 * n, 110 * n, new long[]{40 * n, 50 * n, 60 * n, 70 * n},
                                      new long[]{80 * n, 90 * n}, 80 * n),
                      new Transaction(200 * n, 210 * n, new long[]{60 * n, 75 * n, 78 * n, 100 * n, 110 * n, 120 * n},
                                      new long[]{80 * n, 90 * n}, 80 * n));
    // Prune upper bounds to be returned by the mock plugins
    Iterator<Long> pruneUpperBoundsPlugin1 = Iterators.cycle(60L * n, 80L * n);
    Iterator<Long> pruneUpperBoundsPlugin2 = Iterators.cycle(70L * n, 77L * n);

    TestTransactionPruningRunnable.setCurrentTime(currentTime);
    MockTxManager.setTxIter(txns);
    MockPlugin1.setPruneUpperBoundIter(pruneUpperBoundsPlugin1);
    MockPlugin2.setPruneUpperBoundIter(pruneUpperBoundsPlugin2);

    MockTxManager mockTxManager = new MockTxManager(conf);
    TransactionPruningService pruningService = new TestTransactionPruningService(conf, mockTxManager);
    pruningService.startAndWait();
    // This will cause the pruning run to happen three times,
    // but we are interested in only first two runs for the assertions later
    TimeUnit.SECONDS.sleep(3);
    pruningService.stopAndWait();

    // Assert inactive transaction bound that the plugins receive.
    // Both the plugins should get the same inactive transaction bound since it is
    // computed and passed by the transaction service
    Assert.assertEquals(ImmutableList.of(110L * n - 1, 210L * n - 1),
                        limitTwo(MockPlugin1.getInactiveTransactionBoundList()));
    Assert.assertEquals(ImmutableList.of(110L * n - 1, 210L * n - 1),
                        limitTwo(MockPlugin2.getInactiveTransactionBoundList()));

    // Assert invalid list entries that got pruned
    // The min prune upper bound for the first run should be 60, and for the second run 77
    Assert.assertEquals(ImmutableList.of(ImmutableSet.of(40L * n, 50L * n, 60L * n), ImmutableSet.of(60L * n, 75L * n)),
                        limitTwo(MockTxManager.getPrunedInvalidsList()));

    // Assert max invalid tx pruned that the plugins receive for the prune complete call
    // Both the plugins should get the same max invalid tx pruned value since it is
    // computed and passed by the transaction service
    Assert.assertEquals(ImmutableList.of(60L * n, 75L * n), limitTwo(MockPlugin1.getMaxPrunedInvalidList()));
    Assert.assertEquals(ImmutableList.of(60L * n, 75L * n), limitTwo(MockPlugin2.getMaxPrunedInvalidList()));
  }

  @Test
  public void testNoPruning() throws Exception {
    // Setup plugins
    Configuration conf = new Configuration();
    conf.set(TxConstants.TransactionPruning.PLUGINS,
             "data.tx.txprune.plugin.mockPlugin1, data.tx.txprune.plugin.mockPlugin2");
    conf.set("data.tx.txprune.plugin.mockPlugin1.class",
             "org.apache.tephra.txprune.TransactionPruningServiceTest$MockPlugin1");
    conf.set("data.tx.txprune.plugin.mockPlugin2.class",
             "org.apache.tephra.txprune.TransactionPruningServiceTest$MockPlugin2");
    // Setup schedule to run every second
    conf.setBoolean(TxConstants.TransactionPruning.PRUNE_ENABLE, true);
    conf.setInt(TxConstants.TransactionPruning.PRUNE_INTERVAL, 1);
    conf.setInt(TxConstants.Manager.CFG_TX_MAX_LIFETIME, 10);
    conf.setLong(TxConstants.TransactionPruning.PRUNE_GRACE_PERIOD, 0);

    // Setup mock data
    long m = 1000;
    long n = m * TxConstants.MAX_TX_PER_MS;
    // Current time to be returned
    Iterator<Long> currentTime = Iterators.cycle(120L * m, 220L * m);
    // Transaction objects to be returned by mock tx manager
    Iterator<Transaction> txns =
      Iterators.cycle(new Transaction(100 * n, 110 * n, new long[]{40 * n, 50 * n, 60 * n, 70 * n},
                                      new long[]{80 * n, 90 * n}, 80 * n),
                      new Transaction(200 * n, 210 * n, new long[]{60 * n, 75 * n, 78 * n, 100 * n, 110 * n, 120 * n},
                                      new long[]{80 * n, 90 * n}, 80 * n));
    // Prune upper bounds to be returned by the mock plugins
    Iterator<Long> pruneUpperBoundsPlugin1 = Iterators.cycle(35L * n, -1L);
    Iterator<Long> pruneUpperBoundsPlugin2 = Iterators.cycle(70L * n, 100L * n);

    TestTransactionPruningRunnable.setCurrentTime(currentTime);
    MockTxManager.setTxIter(txns);
    MockPlugin1.setPruneUpperBoundIter(pruneUpperBoundsPlugin1);
    MockPlugin2.setPruneUpperBoundIter(pruneUpperBoundsPlugin2);

    MockTxManager mockTxManager = new MockTxManager(conf);
    TransactionPruningService pruningService = new TestTransactionPruningService(conf, mockTxManager);
    pruningService.startAndWait();
    // This will cause the pruning run to happen three times,
    // but we are interested in only first two runs for the assertions later
    TimeUnit.SECONDS.sleep(3);
    pruningService.stopAndWait();

    // Assert inactive transaction bound
    Assert.assertEquals(ImmutableList.of(110L * n - 1, 210L * n - 1),
                        limitTwo(MockPlugin1.getInactiveTransactionBoundList()));
    Assert.assertEquals(ImmutableList.of(110L * n - 1, 210L * n - 1),
                        limitTwo(MockPlugin2.getInactiveTransactionBoundList()));

    // Invalid entries should not be pruned in any run
    Assert.assertEquals(ImmutableList.of(), MockTxManager.getPrunedInvalidsList());

    // No max invalid tx pruned for any run
    Assert.assertEquals(ImmutableList.of(), limitTwo(MockPlugin1.getMaxPrunedInvalidList()));
    Assert.assertEquals(ImmutableList.of(), limitTwo(MockPlugin2.getMaxPrunedInvalidList()));
  }

  /**
   * Mock transaction manager for testing
   */
  private static class MockTxManager extends TransactionManager {
    private static Iterator<Transaction> txIter;
    private static List<Set<Long>> prunedInvalidsList = new ArrayList<>();

    MockTxManager(Configuration config) {
      super(config);
    }

    @Override
    public Transaction startShort() {
      return txIter.next();
    }

    @Override
    public void abort(Transaction tx) {
      // do nothing
    }

    @Override
    public boolean truncateInvalidTx(Set<Long> invalidTxIds) {
      prunedInvalidsList.add(invalidTxIds);
      return true;
    }

    static void setTxIter(Iterator<Transaction> txIter) {
      MockTxManager.txIter = txIter;
    }

    static List<Set<Long>> getPrunedInvalidsList() {
      return prunedInvalidsList;
    }
  }

  /**
   * Extends {@link TransactionPruningService} to use mock time to help in testing.
   */
  private static class TestTransactionPruningService extends TransactionPruningService {
    TestTransactionPruningService(Configuration conf, TransactionManager txManager) {
      super(conf, txManager);
    }

    @Override
    TransactionPruningRunnable getTxPruneRunnable(TransactionManager txManager,
                                                  Map<String, TransactionPruningPlugin> plugins,
                                                  long txMaxLifetimeMillis, long txPruneBufferMillis) {
      return new TestTransactionPruningRunnable(txManager, plugins, txMaxLifetimeMillis, txPruneBufferMillis);
    }
  }

  /**
   * Extends {@link TransactionPruningRunnable} to use mock time to help in testing.
   */
  private static class TestTransactionPruningRunnable extends TransactionPruningRunnable {
    private static Iterator<Long> currentTime;
    TestTransactionPruningRunnable(TransactionManager txManager, Map<String, TransactionPruningPlugin> plugins,
                                   long txMaxLifetimeMillis, long txPruneBufferMillis) {
      super(txManager, plugins, txMaxLifetimeMillis, txPruneBufferMillis);
    }

    @Override
    long getTime() {
      return currentTime.next();
    }

    static void setCurrentTime(Iterator<Long> currentTime) {
      TestTransactionPruningRunnable.currentTime = currentTime;
    }
  }

  /**
   * Mock transaction pruning plugin for testing.
   */
  private static class MockPlugin1 implements TransactionPruningPlugin {
    private static Iterator<Long> pruneUpperBoundIter;
    private static List<Long> inactiveTransactionBoundList = new ArrayList<>();
    private static List<Long> maxPrunedInvalidList = new ArrayList<>();

    @Override
    public void initialize(Configuration conf) throws IOException {
      // Nothing to do
    }

    @Override
    public long fetchPruneUpperBound(long time, long inactiveTransactionBound) throws IOException {
      inactiveTransactionBoundList.add(inactiveTransactionBound);
      return pruneUpperBoundIter.next();
    }

    @Override
    public void pruneComplete(long time, long maxPrunedInvalid) throws IOException {
      maxPrunedInvalidList.add(maxPrunedInvalid);
    }

    @Override
    public void destroy() {
      // Nothing to do
    }

    static void setPruneUpperBoundIter(Iterator<Long> pruneUpperBoundIter) {
      MockPlugin1.pruneUpperBoundIter = pruneUpperBoundIter;
    }

    static List<Long> getInactiveTransactionBoundList() {
      return inactiveTransactionBoundList;
    }

    static List<Long> getMaxPrunedInvalidList() {
      return maxPrunedInvalidList;
    }
  }

  /**
   * Mock transaction pruning plugin for testing.
   */
  private static class MockPlugin2 implements TransactionPruningPlugin {
    private static Iterator<Long> pruneUpperBoundIter;
    private static List<Long> inactiveTransactionBoundList = new ArrayList<>();
    private static List<Long> maxPrunedInvalidList = new ArrayList<>();

    @Override
    public void initialize(Configuration conf) throws IOException {
      // Nothing to do
    }

    @Override
    public long fetchPruneUpperBound(long time, long inactiveTransactionBound) throws IOException {
      inactiveTransactionBoundList.add(inactiveTransactionBound);
      return pruneUpperBoundIter.next();
    }

    @Override
    public void pruneComplete(long time, long maxPrunedInvalid) throws IOException {
      maxPrunedInvalidList.add(maxPrunedInvalid);
    }

    @Override
    public void destroy() {
      // Nothing to do
    }

    static void setPruneUpperBoundIter(Iterator<Long> pruneUpperBoundIter) {
      MockPlugin2.pruneUpperBoundIter = pruneUpperBoundIter;
    }

    static List<Long> getInactiveTransactionBoundList() {
      return inactiveTransactionBoundList;
    }

    static List<Long> getMaxPrunedInvalidList() {
      return maxPrunedInvalidList;
    }
  }

  private static <T> List<T> limitTwo(Iterable<T> iterable) {
    return ImmutableList.copyOf(Iterables.limit(iterable, 2));
  }
}
