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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.snapshot.SnapshotCodecV4;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Tests the transaction executor.
 */
public class TransactionContextTest {
  private static DummyTxClient txClient;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void setup() throws IOException {
    final Configuration conf = new Configuration();
    conf.set(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES, SnapshotCodecV4.class.getName());
    conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder.newFolder().getAbsolutePath());
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new DiscoveryModules().getInMemoryModules(),
      Modules.override(
        new TransactionModules("clientA").getInMemoryModules()).with(new AbstractModule() {
        @Override
        protected void configure() {
          TransactionManager txManager = new TransactionManager(conf);
          txManager.startAndWait();
          bind(TransactionManager.class).toInstance(txManager);
          bind(TransactionSystemClient.class).to(DummyTxClient.class).in(Singleton.class);
        }
      }));

    txClient = (DummyTxClient) injector.getInstance(TransactionSystemClient.class);
  }

  private final DummyTxAware ds1 = new DummyTxAware(), ds2 = new DummyTxAware();

  private static final byte[] A = { 'a' };
  private static final byte[] B = { 'b' };

  private static TransactionContext newTransactionContext(TransactionAware... txAwares) {
    return new TransactionContext(txClient, txAwares);
  }

  @Before
  public void resetTxAwares() {
    ds1.reset();
    ds2.reset();
  }

  @Test
  public void testSuccessful() throws TransactionFailureException, InterruptedException {
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction
    context.finish();
    // verify both are committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertTrue(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testPostCommitFailure() throws TransactionFailureException, InterruptedException {
    ds1.failPostCommitTxOnce = DummyTxAware.InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail but without rollback as the failure happens post-commit
    try {
      context.finish();
      Assert.fail("post commit failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("post failure", e.getCause().getMessage());
    }
    // verify both are committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertTrue(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testPersistFailure() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = DummyTxAware.InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testPersistFalse() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = DummyTxAware.InduceFailure.ReturnFalse;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertNull(e.getCause()); // in this case, the ds simply returned false
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testPersistAndRollbackFailure() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = DummyTxAware.InduceFailure.ThrowException;
    ds1.failRollbackTxOnce = DummyTxAware.InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is invalidated
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Invalidated);
  }

  @Test
  public void testPersistAndRollbackFalse() throws TransactionFailureException, InterruptedException {
    ds1.failCommitTxOnce = DummyTxAware.InduceFailure.ReturnFalse;
    ds1.failRollbackTxOnce = DummyTxAware.InduceFailure.ReturnFalse;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertNull(e.getCause()); // in this case, the ds simply returned false
    }
    // verify both are rolled back and tx is invalidated
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Invalidated);
  }

  @Test
  public void testCommitFalse() throws TransactionFailureException, InterruptedException {
    txClient.failCommits = 1;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("commit failed - exception should be thrown");
    } catch (TransactionConflictException e) {
      Assert.assertNull(e.getCause());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testCanCommitFalse() throws TransactionFailureException, InterruptedException {
    txClient.failCanCommitOnce = true;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("commit failed - exception should be thrown");
    } catch (TransactionConflictException e) {
      Assert.assertNull(e.getCause());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertFalse(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testChangesAndRollbackFailure() throws TransactionFailureException, InterruptedException {
    ds1.failChangesTxOnce = DummyTxAware.InduceFailure.ThrowException;
    ds1.failRollbackTxOnce = DummyTxAware.InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    context.start();
    // add a change to ds1 and ds2
    ds1.addChange(A);
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("get changes failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("changes failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is invalidated
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertFalse(ds2.checked);
    Assert.assertFalse(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Invalidated);
  }

  @Test
  public void testStartAndRollbackFailure() throws TransactionFailureException, InterruptedException {
    ds1.failStartTxOnce = DummyTxAware.InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext(ds1, ds2);
    // start transaction
    try {
      context.start();
      Assert.fail("start failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("start failure", e.getCause().getMessage());
    }
    // verify both are not rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertFalse(ds2.started);
    Assert.assertFalse(ds1.checked);
    Assert.assertFalse(ds2.checked);
    Assert.assertFalse(ds1.committed);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testAddThenSuccess() throws TransactionFailureException, InterruptedException {
    TransactionContext context = newTransactionContext(ds1);
    // start transaction
    context.start();
    // add a change to ds1
    ds1.addChange(A);
    // add ds2 to the tx
    context.addTransactionAware(ds2);
    // add a change to ds2
    ds2.addChange(B);
    // commit transaction
    context.finish();
    // verify both are committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertTrue(ds2.postCommitted);
    Assert.assertFalse(ds1.rolledBack);
    Assert.assertFalse(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testAddThenFailure() throws TransactionFailureException, InterruptedException {
    ds2.failCommitTxOnce = DummyTxAware.InduceFailure.ThrowException;

    TransactionContext context = newTransactionContext(ds1);
    // start transaction
    context.start();
    // add a change to ds1
    ds1.addChange(A);
    // add ds2 to the tx
    context.addTransactionAware(ds2);
    // add a change to ds2
    ds2.addChange(B);
    // commit transaction should fail and cause rollback
    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }
    // verify both are rolled back and tx is aborted
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds2.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds2.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds2.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertTrue(ds1.rolledBack);
    Assert.assertTrue(ds2.rolledBack);
    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }

  @Test
  public void testAddThenRemoveSuccess() throws TransactionFailureException {
    TransactionContext context = newTransactionContext();

    context.start();
    Assert.assertTrue(context.addTransactionAware(ds1));
    ds1.addChange(A);

    try {
      context.removeTransactionAware(ds1);
      Assert.fail("Removal of TransactionAware should fails when there is active transaction.");
    } catch (IllegalStateException e) {
      // Expected
    }

    context.finish();

    Assert.assertTrue(context.removeTransactionAware(ds1));
    // Removing a TransactionAware not added before should returns false
    Assert.assertFalse(context.removeTransactionAware(ds2));

    // Verify ds1 is committed and post-committed
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertTrue(ds1.postCommitted);
    Assert.assertFalse(ds1.rolledBack);

    // Verify nothing happen to ds2
    Assert.assertFalse(ds2.started);
    Assert.assertFalse(ds2.checked);
    Assert.assertFalse(ds2.committed);
    Assert.assertFalse(ds2.postCommitted);
    Assert.assertFalse(ds2.rolledBack);

    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Committed);
  }

  @Test
  public void testAndThenRemoveOnFailure() throws TransactionFailureException {
    ds1.failCommitTxOnce = DummyTxAware.InduceFailure.ThrowException;
    TransactionContext context = newTransactionContext();

    context.start();
    Assert.assertTrue(context.addTransactionAware(ds1));
    ds1.addChange(A);

    try {
      context.finish();
      Assert.fail("Persist should have failed - exception should be thrown");
    } catch (TransactionFailureException e) {
      Assert.assertEquals("persist failure", e.getCause().getMessage());
    }

    Assert.assertTrue(context.removeTransactionAware(ds1));

    // Verify ds1 is rolled back
    Assert.assertTrue(ds1.started);
    Assert.assertTrue(ds1.checked);
    Assert.assertTrue(ds1.committed);
    Assert.assertFalse(ds1.postCommitted);
    Assert.assertTrue(ds1.rolledBack);

    Assert.assertEquals(txClient.state, DummyTxClient.CommitState.Aborted);
  }
}
