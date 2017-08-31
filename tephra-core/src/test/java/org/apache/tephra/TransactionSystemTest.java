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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionStateStorage;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for testing implementations of {@link TransactionSystemClient}.
 */
public abstract class TransactionSystemTest {

  private static final byte[] C1 = new byte[] { 'c', '1' };
  private static final byte[] C2 = new byte[] { 'c', '2' };
  private static final byte[] C3 = new byte[] { 'c', '3' };
  private static final byte[] C4 = new byte[] { 'c', '4' };

  /**
   * Sets up the common properties required for the test cases defined here.
   * Subclasses can call this and add more properties as needed.
   */
  static Configuration getCommonConfiguration() {
    Configuration conf = new Configuration();
    conf.setInt(TxConstants.Manager.CFG_TX_MAX_TIMEOUT, (int) TimeUnit.DAYS.toSeconds(5)); // very long limit
    conf.setInt(TxConstants.Manager.CFG_TX_CHANGESET_COUNT_LIMIT, 50);
    conf.setInt(TxConstants.Manager.CFG_TX_CHANGESET_COUNT_WARN_THRESHOLD, 40);
    conf.setInt(TxConstants.Manager.CFG_TX_CHANGESET_SIZE_LIMIT, 2048);
    conf.setInt(TxConstants.Manager.CFG_TX_CHANGESET_SIZE_WARN_THRESHOLD, 1024);
    return conf;
  }

  protected abstract TransactionSystemClient getClient() throws Exception;

  protected abstract TransactionStateStorage getStateStorage() throws Exception;

  @Test // can't do (expected=IllegalArgumentException) because the subclass needs to perform an extra assert
  public void testNegativeTimeout() throws Exception {
    try {
      getClient().startShort(-1);
      Assert.fail("Expected illegal argument for negative timeout");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test // can't do (expected=IllegalArgumentException) because the subclass needs to perform an extra assert
  public void testExcessiveTimeout() throws Exception {
    try {
      getClient().startShort((int) TimeUnit.DAYS.toSeconds(10));
      Assert.fail("Expected illegal argument for excessive timeout");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testLargeChangeSet() throws Exception {
    TransactionSystemClient client = getClient();
    // first try with 50 changes (the max allowed)
    List<byte[]> fiftyChanges = new ArrayList<>(51);
    for (byte i = 0; i < 50; i++) {
      fiftyChanges.add(new byte[] { i });
    }
    Transaction tx = client.startShort();
    client.canCommitOrThrow(tx, fiftyChanges);
    client.commit(tx);

    // now try another transaction with 51 changes
    fiftyChanges.add(new byte[] { 50 });
    tx = client.startShort();
    try {
      client.canCommitOrThrow(tx, fiftyChanges);
      Assert.fail("Expected " + TransactionSizeException.class.getName());
    } catch (TransactionSizeException e) {
      // expected
    }
    client.abort(tx);

    // now try a change set that is just within the size limit
    List<byte[]> changes2k = new ArrayList<>(51);
    for (byte i = 0; i < 8; i++) {
      byte[] change = new byte[256];
      change[0] = i;
      changes2k.add(change);
    }
    tx = client.startShort();
    client.canCommitOrThrow(tx, changes2k);
    client.commit(tx);

    // now add another byte to the change set to exceed the limit
    changes2k.add(new byte[] { 0 });
    tx = client.startShort();
    try {
      client.canCommitOrThrow(tx, changes2k);
      Assert.fail("Expected " + TransactionSizeException.class.getName());
    } catch (TransactionSizeException e) {
      // expected
    }
    client.abort(tx);
  }

  @Test
  public void testCommitRaceHandling() throws Exception {
    TransactionSystemClient client1 = getClient();
    TransactionSystemClient client2 = getClient();

    Transaction tx1 = client1.startShort();
    Transaction tx2 = client2.startShort();

    Assert.assertTrue(client1.canCommitOrThrow(tx1, asList(C1, C2)));
    // second one also can commit even thought there are conflicts with first since first one hasn't committed yet
    Assert.assertTrue(client2.canCommitOrThrow(tx2, asList(C2, C3)));

    Assert.assertTrue(client1.commit(tx1));

    // now second one should not commit, since there are conflicts with tx1 that has been committed
    Assert.assertFalse(client2.commit(tx2));
  }

  @Test
  public void testMultipleCommitsAtSameTime() throws Exception {
    // We want to check that if two txs finish at same time (wrt tx manager) they do not overwrite changesets of each
    // other in tx manager used for conflicts detection (we had this bug)
    // NOTE: you don't have to use multiple clients for that
    TransactionSystemClient client1 = getClient();
    TransactionSystemClient client2 = getClient();
    TransactionSystemClient client3 = getClient();
    TransactionSystemClient client4 = getClient();
    TransactionSystemClient client5 = getClient();

    Transaction tx1 = client1.startShort();
    Transaction tx2 = client2.startShort();
    Transaction tx3 = client3.startShort();
    Transaction tx4 = client4.startShort();
    Transaction tx5 = client5.startShort();

    Assert.assertTrue(client1.canCommitOrThrow(tx1, asList(C1)));
    Assert.assertTrue(client1.commit(tx1));

    Assert.assertTrue(client2.canCommitOrThrow(tx2, asList(C2)));
    Assert.assertTrue(client2.commit(tx2));

    // verifying conflicts detection
    Assert.assertFalse(client3.canCommitOrThrow(tx3, asList(C1)));
    Assert.assertFalse(client4.canCommitOrThrow(tx4, asList(C2)));
    Assert.assertTrue(client5.canCommitOrThrow(tx5, asList(C3)));
  }

  @Test
  public void testCommitTwice() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommitOrThrow(tx, asList(C1, C2)));
    Assert.assertTrue(client.commit(tx));
    // cannot commit twice same tx
    try {
      Assert.assertFalse(client.commit(tx));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
  }

  @Test
  public void testAbortTwice() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommitOrThrow(tx, asList(C1, C2)));
    client.abort(tx);
    // abort of not active tx has no affect
    client.abort(tx);
  }

  @Test
  public void testReuseTx() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommitOrThrow(tx, asList(C1, C2)));
    Assert.assertTrue(client.commit(tx));
    // can't re-use same tx again
    try {
      client.canCommitOrThrow(tx, asList(C3, C4));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    try {
      Assert.assertFalse(client.commit(tx));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }

    // abort of not active tx has no affect
    client.abort(tx);
  }

  @Test
  public void testUseNotStarted() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx1 = client.startShort();
    Assert.assertTrue(client.commit(tx1));

    // we know this is one is older than current writePointer and was not used
    Transaction txOld = new Transaction(tx1.getReadPointer(), tx1.getTransactionId() - 1,
                                        new long[] {}, new long[] {}, Transaction.NO_TX_IN_PROGRESS, 
                                        TransactionType.SHORT);
    try {
      Assert.assertFalse(client.canCommitOrThrow(txOld, asList(C3, C4)));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    try {
      Assert.assertFalse(client.commit(txOld));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    // abort of not active tx has no affect
    client.abort(txOld);

    // we know this is one is newer than current readPointer and was not used
    Transaction txNew = new Transaction(tx1.getReadPointer(), tx1.getTransactionId() + 1,
                                        new long[] {}, new long[] {}, Transaction.NO_TX_IN_PROGRESS, 
                                        TransactionType.SHORT);
    try {
      Assert.assertFalse(client.canCommitOrThrow(txNew, asList(C3, C4)));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    try {
      Assert.assertFalse(client.commit(txNew));
      Assert.fail();
    } catch (TransactionNotInProgressException e) {
      // expected
    }
    // abort of not active tx has no affect
    client.abort(txNew);
  }

  @Test
  public void testAbortAfterCommit() throws Exception {
    TransactionSystemClient client = getClient();
    Transaction tx = client.startShort();

    Assert.assertTrue(client.canCommitOrThrow(tx, asList(C1, C2)));
    Assert.assertTrue(client.commit(tx));
    // abort of not active tx has no affect
    client.abort(tx);
  }

  // todo add test invalidate method
  @Test
  public void testInvalidateTx() throws Exception {
    TransactionSystemClient client = getClient();
    // Invalidate an in-progress tx
    Transaction tx1 = client.startShort();
    client.canCommitOrThrow(tx1, asList(C1, C2));
    Assert.assertTrue(client.invalidate(tx1.getTransactionId()));
    // Cannot invalidate a committed tx
    Transaction tx2 = client.startShort();
    client.canCommitOrThrow(tx2, asList(C3, C4));
    client.commit(tx2);
    Assert.assertFalse(client.invalidate(tx2.getTransactionId()));
  }

  @Test
  public void testResetState() throws Exception {
    // have tx in progress, committing and committed then reset,
    // get the last snapshot and see that it is empty
    TransactionSystemClient client = getClient();
    TransactionStateStorage stateStorage = getStateStorage();

    Transaction tx1 = client.startShort();
    Transaction tx2 = client.startShort();
    client.canCommitOrThrow(tx1, asList(C1, C2));
    client.commit(tx1);
    client.canCommitOrThrow(tx2, asList(C3, C4));

    Transaction txPreReset = client.startShort();
    long currentTs = System.currentTimeMillis();
    client.resetState();

    TransactionSnapshot snapshot = stateStorage.getLatestSnapshot();
    Assert.assertTrue(snapshot.getTimestamp() >= currentTs);
    Assert.assertEquals(0, snapshot.getInvalid().size());
    Assert.assertEquals(0, snapshot.getInProgress().size());
    Assert.assertEquals(0, snapshot.getCommittingChangeSets().size());
    Assert.assertEquals(0, snapshot.getCommittedChangeSets().size());

    // confirm that transaction IDs are not reset
    Transaction txPostReset = client.startShort();
    Assert.assertTrue("New tx ID should be greater than last ID before reset",
                      txPostReset.getTransactionId() > txPreReset.getTransactionId());
  }
  
  @Test
  public void testTruncateInvalidTx() throws Exception {
    // Start few transactions and invalidate all of them
    TransactionSystemClient client = getClient();
    Transaction tx1 = client.startLong();
    Transaction tx2 = client.startShort();
    Transaction tx3 = client.startLong();
    
    client.invalidate(tx1.getTransactionId());
    client.invalidate(tx2.getTransactionId());
    client.invalidate(tx3.getTransactionId());
    
    // Remove tx2 and tx3 from invalid list
    Assert.assertTrue(client.truncateInvalidTx(ImmutableSet.of(tx2.getTransactionId(), tx3.getTransactionId())));
    
    Transaction tx = client.startShort();
    // Only tx1 should be in invalid list now
    Assert.assertArrayEquals(new long[] {tx1.getTransactionId()}, tx.getInvalids());
    client.abort(tx);
  }

  @Test
  public void testTruncateInvalidTxBefore() throws Exception {
    // Start few transactions
    TransactionSystemClient client = getClient();
    Transaction tx1 = client.startLong();
    Transaction tx2 = client.startShort();
    // Sleep so that transaction ids get generated a millisecond apart for assertion
    // TEPHRA-63 should eliminate the need to sleep
    TimeUnit.MILLISECONDS.sleep(1);
    long beforeTx3 = System.currentTimeMillis();
    Transaction tx3 = client.startLong();
    
    try {
      // throws exception since tx1 and tx2 are still in-progress
      client.truncateInvalidTxBefore(beforeTx3);
      Assert.fail("Expected InvalidTruncateTimeException exception");
    } catch (InvalidTruncateTimeException e) {
      // Expected exception
    }
    
    // Invalidate all of them
    client.invalidate(tx1.getTransactionId());
    client.invalidate(tx2.getTransactionId());
    client.invalidate(tx3.getTransactionId());

    // Remove transactions before time beforeTx3
    Assert.assertTrue(client.truncateInvalidTxBefore(beforeTx3));

    Transaction tx = client.startShort();
    // Only tx3 should be in invalid list now
    Assert.assertArrayEquals(new long[] {tx3.getTransactionId()}, tx.getInvalids());
    client.abort(tx);
  }

  @Test
  public void testGetInvalidSize() throws Exception {
    // Start few transactions and invalidate all of them
    TransactionSystemClient client = getClient();
    Transaction tx1 = client.startLong();
    Transaction tx2 = client.startShort();
    Transaction tx3 = client.startLong();

    Assert.assertEquals(0, client.getInvalidSize());

    client.invalidate(tx1.getTransactionId());
    client.invalidate(tx2.getTransactionId());
    client.invalidate(tx3.getTransactionId());

    Assert.assertEquals(3, client.getInvalidSize());
  }

  @Test
  public void testCheckpointing() throws Exception {
    TransactionSystemClient client = getClient();
    // create a few transactions
    Transaction tx1 = client.startShort();
    Transaction tx2 = client.startShort();
    Transaction tx3 = client.startShort();

    // start and commit a few
    for (int i = 0; i < 5; i++) {
      Transaction tx = client.startShort();
      Assert.assertTrue(client.canCommit(tx, Collections.singleton(new byte[] { (byte) i })));
      Assert.assertTrue(client.commit(tx));
    }

    // checkpoint the transactions
    Transaction tx3c = client.checkpoint(tx3);
    Transaction tx2c = client.checkpoint(tx2);
    Transaction tx1c = client.checkpoint(tx1);

    // start and commit a few (this moves the read pointer past all checkpoint write versions)
    for (int i = 5; i < 10; i++) {
      Transaction tx = client.startShort();
      Assert.assertTrue(client.canCommit(tx, Collections.singleton(new byte[] { (byte) i })));
      Assert.assertTrue(client.commit(tx));
    }

    // start new tx and validate all write pointers are excluded
    Transaction tx = client.startShort();
    validateSorted(tx.getInProgress());
    validateSorted(tx.getInvalids());
    Assert.assertFalse(tx.isVisible(tx1.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx2.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx3.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx1c.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx2c.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx3c.getWritePointer()));
    client.abort(tx);

    // abort one of the checkpoints
    client.abort(tx1c);

    // start new tx and validate all write pointers are excluded
    tx = client.startShort();
    validateSorted(tx.getInProgress());
    validateSorted(tx.getInvalids());
    Assert.assertFalse(tx.isVisible(tx2.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx3.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx2c.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx3c.getWritePointer()));
    client.abort(tx);

    // invalidate one of the checkpoints
    client.invalidate(tx2c.getTransactionId());

    // start new tx and validate all write pointers are excluded
    tx = client.startShort();
    validateSorted(tx.getInProgress());
    validateSorted(tx.getInvalids());
    Assert.assertFalse(tx.isVisible(tx2.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx3.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx2c.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx3c.getWritePointer()));
    client.abort(tx);

    // commit the last checkpoint
    Assert.assertTrue(client.canCommit(tx3, Collections.<byte[]>emptyList()));
    Assert.assertTrue(client.commit(tx3c));

    // start new tx and validate all write pointers are excluded
    tx = client.startShort();
    validateSorted(tx.getInProgress());
    validateSorted(tx.getInvalids());
    Assert.assertFalse(tx.isVisible(tx2.getWritePointer()));
    Assert.assertFalse(tx.isVisible(tx2c.getWritePointer()));
    client.abort(tx);
  }

  private void validateSorted(long[] array) {
    Long lastSeen = null;
    for (long value : array) {
      Assert.assertTrue(String.format("%s is not sorted", Arrays.toString(array)),
                        lastSeen == null || lastSeen < value);
      lastSeen = value;
    }
  }

  private Collection<byte[]> asList(byte[]... val) {
    return Arrays.asList(val);
  }
}
