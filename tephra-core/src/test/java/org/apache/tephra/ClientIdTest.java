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
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.metrics.TxMetricsCollector;
import org.apache.tephra.persist.InMemoryTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the retention of client ids in Tx Manager
 */
@SuppressWarnings("WeakerAccess")
public class ClientIdTest {

  @Test
  public void testClientIdRetention() throws TransactionFailureException {
    testClientIdRetention(TransactionManager.ClientIdRetention.OFF, false, false);
    testClientIdRetention(TransactionManager.ClientIdRetention.ACTIVE, true, false);
    testClientIdRetention(TransactionManager.ClientIdRetention.COMMITTED, true, true);
  }

  private void testClientIdRetention(TransactionManager.ClientIdRetention retention,
                                     boolean expectClientIdInProgress,
                                     boolean expectClientIdCommitted) throws TransactionFailureException {
    Configuration conf = new Configuration();
    conf.set(TxConstants.Manager.CFG_TX_RETAIN_CLIENT_ID, retention.toString());
    TransactionStateStorage txStateStorage = new InMemoryTransactionStateStorage();
    TransactionManager txManager = new TransactionManager(conf, txStateStorage, new TxMetricsCollector());
    txManager.startAndWait();
    try {
      testConflict(txManager, expectClientIdInProgress, expectClientIdCommitted);
    } finally {
      txManager.stopAndWait();
    }
  }

  public void testConflict(TransactionManager txManager,
                           boolean expectClientIdInProgress,
                           boolean expectClientIdCommitted) throws TransactionFailureException {
    testConflict(txManager, expectClientIdInProgress, expectClientIdCommitted, true);
    testConflict(txManager, expectClientIdInProgress, expectClientIdCommitted, false);
  }

  /**
   * Tests two conflicting transactions.
   * The resulting exception must carry the conflicting change key and client id.
   *
   * @param expectClientIdInProgress whether to expect client id in in-progress transactions
   * @param expectClientIdCommitted whether  to expect client id in committed chaneg sets
   * @param testCanCommit whether the conflict should be induced by canCommit() or by commit()
   */
  public void testConflict(TransactionManager txManager,
                           boolean expectClientIdInProgress,
                           boolean expectClientIdCommitted,
                           boolean testCanCommit) throws TransactionFailureException {
    // start two transactions, validate client id
    Transaction tx1 = txManager.startShort("clientA");
    Transaction tx2 = txManager.startShort("clientB");
    TransactionManager.InProgressTx inProgressTx1 = txManager.getInProgress(tx1.getTransactionId());
    Assert.assertNotNull(inProgressTx1);
    if (expectClientIdInProgress) {
      Assert.assertEquals("clientA", inProgressTx1.getClientId());
    } else {
      Assert.assertNull(inProgressTx1.getClientId());
    }

    // now commit the two transactions with overlapping change sets to create a conflict
    final byte[] change1 = new byte[] { '1' };
    final byte[] change2 = new byte[] { '2' };
    final byte[] change3 = new byte[] { '3' };
    if (!testCanCommit) {
      txManager.canCommit(tx2.getTransactionId(), ImmutableList.of(change2, change3));
    }
    txManager.canCommit(tx1.getTransactionId(), ImmutableList.of(change1, change2));
    txManager.commit(tx1.getTransactionId(), tx1.getWritePointer());
    try {
      if (testCanCommit) {
        txManager.canCommit(tx2.getTransactionId(), ImmutableList.of(change2, change3));
      } else {
        txManager.commit(tx2.getTransactionId(), tx2.getWritePointer());
      }
      Assert.fail("canCommit() should have failed with conflict");
    } catch (TransactionConflictException e) {
      Assert.assertNotNull(e.getTransactionId());
      Assert.assertEquals(tx2.getTransactionId(), e.getTransactionId().longValue());
      Assert.assertEquals("2", e.getConflictingKey());
      if (expectClientIdCommitted) {
        Assert.assertEquals("clientA", e.getConflictingClient());
      } else {
        Assert.assertNull(e.getConflictingClient());
      }
    }
  }
}
