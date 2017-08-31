/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.InMemoryTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TransactionManagerTest extends TransactionSystemTest {

  private static Configuration conf;

  private static TransactionManager txManager = null;
  private static TransactionStateStorage txStateStorage = null;

  @Override
  protected TransactionSystemClient getClient() {
    return new InMemoryTxSystemClient(txManager);
  }

  @Override
  protected TransactionStateStorage getStateStorage() {
    return txStateStorage;
  }

  @BeforeClass
  public static void beforeClass() {
    conf = getCommonConfiguration();
    conf.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 0); // no cleanup thread
    // todo should create two sets of tests, one with LocalFileTxStateStorage and one with InMemoryTxStateStorage
    txStateStorage = new InMemoryTransactionStateStorage();
    txManager = new TransactionManager
      (conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();
  }

  @AfterClass
  public static void afterClass() {
    txManager.stopAndWait();
  }

  @After
  public void after() {
    txManager.resetState();
  }

  @Test
  public void testTransactionCleanup() throws Exception {
    Configuration config = new Configuration(conf);
    config.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 3);
    config.setInt(TxConstants.Manager.CFG_TX_TIMEOUT, 2);
    // using a new tx manager that cleans up
    TransactionManager txm = new TransactionManager
      (config, new InMemoryTransactionStateStorage(), new TxMetricsCollector());
    txm.startAndWait();
    try {
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // start two transactions and leave them open
      Transaction tx1 = txm.startShort();
      Transaction tx2 = txm.startShort();
      // start two long running transactions and leave them open
      Transaction ltx1 = txm.startLong();
      Transaction ltx2 = txm.startLong();
      // checkpoint one of the short transactions
      Transaction tx2c = txm.checkpoint(tx2);
      // start and commit a bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.startShort();
        Assert.assertTrue(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })));
        Assert.assertTrue(txm.commit(tx));
      }
      // all of these should still be in the committed set
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(10, txm.getCommittedSize());

      // sleep longer than the cleanup interval
      TimeUnit.SECONDS.sleep(5);
      // transaction should now be invalid
      //Assert.assertEquals(3, txm.getInvalidSize());
      // run another transaction
      Transaction txx = txm.startShort();
      // verify the exclude
      Assert.assertFalse(txx.isVisible(tx1.getWritePointer()));
      Assert.assertFalse(txx.isVisible(tx2.getWritePointer()));
      Assert.assertFalse(txx.isVisible(tx2c.getWritePointer()));
      Assert.assertFalse(txx.isVisible(ltx1.getWritePointer()));
      Assert.assertFalse(txx.isVisible(ltx2.getWritePointer()));
      // verify all of  the short write pointers are in the invalid list
      Assert.assertEquals(3, txx.getInvalids().length);
      Assert.assertArrayEquals(new long[] {
                                 tx1.getWritePointer(),
                                 tx2.getWritePointer(),
                                 tx2c.getWritePointer()}, txx.getInvalids());
      // try to commit the last transaction that was started
      Assert.assertTrue(txm.canCommit(txx, Collections.singleton(new byte[] { 0x0a })));
      Assert.assertTrue(txm.commit(txx));

      // now the committed change sets should be empty again
      Assert.assertEquals(0, txm.getCommittedSize());
      // cannot commit transaction as it was timed out
      try {
        txm.canCommit(tx1, Collections.singleton(new byte[] { 0x11 }));
        Assert.fail();
      } catch (TransactionNotInProgressException e) {
        // expected
      }

      // abort should remove tx1 from invalid, but tx2 and tx2c are still there
      txm.abort(tx1);
      Assert.assertEquals(2, txm.getInvalidSize());

      // aborting tx2c should remove both tx2 and tx2c from invalids
      txm.abort(tx2c);
      Assert.assertEquals(0, txm.getInvalidSize());

      // run another bunch of transactions
      for (int i = 0; i < 10; i++) {
        Transaction tx = txm.startShort();
        Assert.assertTrue(txm.canCommit(tx, Collections.singleton(new byte[] { (byte) i })));
        Assert.assertTrue(txm.commit(tx));
      }
      // none of these should still be in the committed set (tx2 is long-running).
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      // commit tx2, abort tx3
      Assert.assertTrue(txm.commit(ltx1));
      txm.abort(ltx2);
      // none of these should still be in the committed set (tx2 is long-running).
      // Only tx3 is invalid list as it was aborted and is long-running. tx1 is short one and it rolled back its changes
      // so it should NOT be in invalid list
      Assert.assertEquals(1, txm.getInvalidSize());
      Assert.assertEquals(ltx2.getTransactionId(), (long) txm.getCurrentState().getInvalid().iterator().next());
      Assert.assertEquals(1, txm.getExcludedListSize());
    } finally {
      txm.stopAndWait();
    }
  }

  @Test
  public void testLongTransactionCleanup() throws Exception {
    Configuration config = new Configuration(conf);
    config.setInt(TxConstants.Manager.CFG_TX_CLEANUP_INTERVAL, 3);
    config.setInt(TxConstants.Manager.CFG_TX_LONG_TIMEOUT, 2);
    // using a new tx manager that cleans up
    TransactionManager txm = new TransactionManager
      (config, new InMemoryTransactionStateStorage(), new TxMetricsCollector());
    txm.startAndWait();
    try {
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());
      
      // start a long running transaction
      Transaction tx1 = txm.startLong();
      
      Assert.assertEquals(0, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());

      // sleep longer than the cleanup interval
      TimeUnit.SECONDS.sleep(5);

      // transaction should now be invalid
      Assert.assertEquals(1, txm.getInvalidSize());
      Assert.assertEquals(0, txm.getCommittedSize());

      // cannot commit transaction as it was timed out
      try {
        txm.canCommit(tx1, Collections.singleton(new byte[] { 0x11 }));
        Assert.fail();
      } catch (TransactionNotInProgressException e) {
        // expected
      }
      
      txm.abort(tx1);
      // abort should not remove long running transaction from invalid list
      Assert.assertEquals(1, txm.getInvalidSize());
    } finally {
      txm.stopAndWait();
    }
  }
  
  @Test
  public void testTruncateInvalid() throws Exception {
    InMemoryTransactionStateStorage storage = new InMemoryTransactionStateStorage();
    Configuration testConf = new Configuration(conf);
    // No snapshots
    testConf.setLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, -1);
    TransactionManager txm1 = new TransactionManager(testConf, storage, new TxMetricsCollector());
    txm1.startAndWait();

    TransactionManager txm2 = null;
    Transaction tx1;
    Transaction tx2;
    Transaction tx3;
    Transaction tx4;
    Transaction tx5;
    Transaction tx6;
    try {
      Assert.assertEquals(0, txm1.getInvalidSize());

      // start a few transactions
      tx1 = txm1.startLong();
      tx2 = txm1.startShort();
      tx3 = txm1.startLong();
      tx4 = txm1.startShort();
      tx5 = txm1.startLong();
      tx6 = txm1.startShort();

      // invalidate tx1, tx2, tx5 and tx6
      txm1.invalidate(tx1.getTransactionId());
      txm1.invalidate(tx2.getTransactionId());
      txm1.invalidate(tx5.getTransactionId());
      txm1.invalidate(tx6.getTransactionId());

      // tx1, tx2, tx5 and tx6 should be in invalid list
      Assert.assertEquals(
        ImmutableList.of(tx1.getTransactionId(), tx2.getTransactionId(), tx5.getTransactionId(),
            tx6.getTransactionId()),
        txm1.getCurrentState().getInvalid()
      );
      
      // remove tx1 and tx6 from invalid list
      Assert.assertTrue(txm1.truncateInvalidTx(ImmutableSet.of(tx1.getTransactionId(), tx6.getTransactionId())));
      
      // only tx2 and tx5 should be in invalid list now
      Assert.assertEquals(ImmutableList.of(tx2.getTransactionId(), tx5.getTransactionId()),
                          txm1.getCurrentState().getInvalid());
      
      // removing in-progress transactions should not have any effect
      Assert.assertEquals(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId()),
                          txm1.getCurrentState().getInProgress().keySet());
      Assert.assertFalse(txm1.truncateInvalidTx(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId())));
      // no change to in-progress
      Assert.assertEquals(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId()),
                          txm1.getCurrentState().getInProgress().keySet());
      // no change to invalid list
      Assert.assertEquals(ImmutableList.of(tx2.getTransactionId(), tx5.getTransactionId()),
                          txm1.getCurrentState().getInvalid());

      // Test transaction edit logs replay
      // Start another transaction manager without stopping txm1 so that snapshot does not get written,
      // and all logs can be replayed.
      txm2 = new TransactionManager(testConf, storage, new TxMetricsCollector());
      txm2.startAndWait();
      Assert.assertEquals(ImmutableList.of(tx2.getTransactionId(), tx5.getTransactionId()),
                          txm2.getCurrentState().getInvalid());
      Assert.assertEquals(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId()),
                          txm2.getCurrentState().getInProgress().keySet());
    } finally {
      txm1.stopAndWait();
      if (txm2 != null) {
        txm2.stopAndWait();
      }
    }
  }

  @Test
  public void testTruncateInvalidBeforeTime() throws Exception {
    InMemoryTransactionStateStorage storage = new InMemoryTransactionStateStorage();
    Configuration testConf = new Configuration(conf);
    // No snapshots
    testConf.setLong(TxConstants.Manager.CFG_TX_SNAPSHOT_INTERVAL, -1);
    TransactionManager txm1 = new TransactionManager(testConf, storage, new TxMetricsCollector());
    txm1.startAndWait();

    TransactionManager txm2 = null;
    Transaction tx1;
    Transaction tx2;
    Transaction tx3;
    Transaction tx4;
    Transaction tx5;
    Transaction tx6;
    try {
      Assert.assertEquals(0, txm1.getInvalidSize());

      // start a few transactions
      tx1 = txm1.startLong();
      tx2 = txm1.startShort();
      // Sleep so that transaction ids get generated a millisecond apart for assertion
      // TEPHRA-63 should eliminate the need to sleep
      TimeUnit.MILLISECONDS.sleep(1);
      long timeBeforeTx3 = System.currentTimeMillis();
      tx3 = txm1.startLong();
      tx4 = txm1.startShort();
      TimeUnit.MILLISECONDS.sleep(1);
      long timeBeforeTx5 = System.currentTimeMillis();
      tx5 = txm1.startLong();
      tx6 = txm1.startShort();

      // invalidate tx1, tx2, tx5 and tx6
      txm1.invalidate(tx1.getTransactionId());
      txm1.invalidate(tx2.getTransactionId());
      txm1.invalidate(tx5.getTransactionId());
      txm1.invalidate(tx6.getTransactionId());

      // tx1, tx2, tx5 and tx6 should be in invalid list
      Assert.assertEquals(
        ImmutableList.of(tx1.getTransactionId(), tx2.getTransactionId(), tx5.getTransactionId(),
            tx6.getTransactionId()),
        txm1.getCurrentState().getInvalid()
      );

      // remove transactions before tx3 from invalid list
      Assert.assertTrue(txm1.truncateInvalidTxBefore(timeBeforeTx3));

      // only tx5 and tx6 should be in invalid list now
      Assert.assertEquals(ImmutableList.of(tx5.getTransactionId(), tx6.getTransactionId()),
                          txm1.getCurrentState().getInvalid());

      // removing invalid transactions before tx5 should throw exception since tx3 and tx4 are in-progress
      Assert.assertEquals(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId()),
                          txm1.getCurrentState().getInProgress().keySet());
      try {
        txm1.truncateInvalidTxBefore(timeBeforeTx5);
        Assert.fail("Expected InvalidTruncateTimeException exception");
      } catch (InvalidTruncateTimeException e) {
        // Expected exception
      }
      // no change to in-progress
      Assert.assertEquals(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId()),
                          txm1.getCurrentState().getInProgress().keySet());
      // no change to invalid list
      Assert.assertEquals(ImmutableList.of(tx5.getTransactionId(), tx6.getTransactionId()),
                          txm1.getCurrentState().getInvalid());

      // Test transaction edit logs replay
      // Start another transaction manager without stopping txm1 so that snapshot does not get written, 
      // and all logs can be replayed.
      txm2 = new TransactionManager(testConf, storage, new TxMetricsCollector());
      txm2.startAndWait();
      Assert.assertEquals(ImmutableList.of(tx5.getTransactionId(), tx6.getTransactionId()),
                          txm2.getCurrentState().getInvalid());
      Assert.assertEquals(ImmutableSet.of(tx3.getTransactionId(), tx4.getTransactionId()),
                          txm2.getCurrentState().getInProgress().keySet());
    } finally {
      txm1.stopAndWait();
      if (txm2 != null) {
        txm2.stopAndWait();
      }
    }
  }
}
