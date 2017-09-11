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

package org.apache.tephra.distributed;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.ThriftTransactionSystemTest;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.tephra.persist.InMemoryTransactionStateStorage;
import org.apache.tephra.persist.TransactionEdit;
import org.apache.tephra.persist.TransactionLog;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionClientModule;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.runtime.ZKModule;
import org.apache.tephra.util.Tests;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This tests whether transaction service hangs on stop when heavily loaded - https://issues.cask.co/browse/TEPHRA-132
 * as well as proper handling of zk election https://issues.cask.co/browse/TEPHRA-179.
 */
public class ThriftTransactionServerTest {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftTransactionSystemTest.class);

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;
  private static TransactionService txService;
  private static TransactionStateStorage storage;
  private static Injector injector;

  private static final int NUM_CLIENTS = 17;
  // storageWaitLatch is used to simulate slow HDFS writes for TEPHRA-132
  private static CountDownLatch storageWaitLatch;
  private static CountDownLatch clientsDoneLatch;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void start() throws Exception {
    zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    Configuration conf = new Configuration();
    conf.setBoolean(TxConstants.Manager.CFG_DO_PERSIST, false);
    conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
    conf.set(TxConstants.Service.CFG_DATA_TX_CLIENT_RETRY_STRATEGY, "n-times");
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_ATTEMPTS, 1);
    conf.setInt(TxConstants.Service.CFG_DATA_TX_CLIENT_COUNT, NUM_CLIENTS);
    conf.setLong(TxConstants.Service.CFG_DATA_TX_CLIENT_TIMEOUT, TimeUnit.HOURS.toMillis(1));
    conf.setInt(TxConstants.Service.CFG_DATA_TX_SERVER_IO_THREADS, 2);
    conf.setInt(TxConstants.Service.CFG_DATA_TX_SERVER_THREADS, 4);
    conf.setInt(TxConstants.HBase.ZK_SESSION_TIMEOUT, 10000);

    injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      Modules.override(new TransactionModules().getDistributedModules())
        .with(new AbstractModule() {
          @Override
          protected void configure() {
            bind(TransactionStateStorage.class).to(SlowTransactionStorage.class).in(Scopes.SINGLETON);
            // overriding this to make it non-singleton
            bind(TransactionSystemClient.class).to(TransactionServiceClient.class);
          }
        }),
      new TransactionClientModule()
    );

    zkClientService = injector.getInstance(ZKClientService.class);
    zkClientService.startAndWait();

    // start a tx server
    txService = injector.getInstance(TransactionService.class);
    storage = injector.getInstance(TransactionStateStorage.class);
    try {
      LOG.info("Starting transaction service");
      txService.startAndWait();
    } catch (Exception e) {
      LOG.error("Failed to start service: ", e);
      throw e;
    }

    Tests.waitForTxReady(injector.getInstance(TransactionSystemClient.class));

    getClient().resetState();

    storageWaitLatch = new CountDownLatch(1);
    clientsDoneLatch = new CountDownLatch(NUM_CLIENTS);
  }

  @After
  public void stop() throws Exception {
    txService.stopAndWait();
    storage.stopAndWait();
    zkClientService.stopAndWait();
    zkServer.stopAndWait();
  }

  public TransactionSystemClient getClient() throws Exception {
    return injector.getInstance(TransactionSystemClient.class);
  }

  @Test
  public void testThriftServerStop() throws Exception {
    Assert.assertEquals(Service.State.RUNNING, txService.thriftRPCServerState());

    int nThreads = NUM_CLIENTS;
    ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
    for (int i = 0; i < nThreads; ++i) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            TransactionSystemClient txClient = getClient();
            clientsDoneLatch.countDown();
            // this will hang, due to the slow edit log (until the latch in it is stopped)
            txClient.startShort();
          } catch (Exception e) {
            // Exception expected
          }
        }
      });
    }

    // Wait till all clients finish sending request to transaction manager
    clientsDoneLatch.await();
    TimeUnit.SECONDS.sleep(1);

    // Expire zookeeper session, which causes Thrift server to stop.
    expireZkSession(zkClientService);
    waitForThriftStop();

    // Stop Zookeeper client so that it does not re-connect to Zookeeper and start Thrift server again.
    zkClientService.stopAndWait();
    storageWaitLatch.countDown();
    TimeUnit.SECONDS.sleep(1);

    // Make sure Thrift server stopped.
    Assert.assertEquals(Service.State.TERMINATED, txService.thriftRPCServerState());
  }

  @Test
  public void testThriftServerRestart() throws Exception {
    // we don't need a slow Transaction Log for this test case
    storageWaitLatch.countDown();
    Assert.assertEquals(Service.State.RUNNING, txService.thriftRPCServerState());

    // simply start + commit transaction
    TransactionSystemClient txClient = getClient();
    Transaction tx = txClient.startShort();
    txClient.commitOrThrow(tx);

    // Expire zookeeper session, which causes Thrift server to stop running.
    expireZkSession(zkClientService);
    waitForThriftStop();

    // wait for the thrift rpc server to be in running state again
    Tests.waitFor("Failed to wait for txService to be running.", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return Service.State.RUNNING == txService.thriftRPCServerState();
      }
    });

    // we need to get a new txClient, because the old one will no longer work after the thrift server restart
    txClient = getClient();
    // verify that we can start and commit a transaction after becoming leader again
    tx = txClient.startShort();
    txClient.commitOrThrow(tx);
  }

  private void expireZkSession(ZKClientService zkClientService) throws Exception {
    ZooKeeper zooKeeper = zkClientService.getZooKeeperSupplier().get();
    final SettableFuture<?> connectFuture = SettableFuture.create();
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
          connectFuture.set(null);
        }
      }
    };

    // Create another Zookeeper session with the same sessionId so that the original one expires.
    ZooKeeper dupZookeeper =
      new ZooKeeper(zkClientService.getConnectString(), zooKeeper.getSessionTimeout(), watcher,
                    zooKeeper.getSessionId(), zooKeeper.getSessionPasswd());
    connectFuture.get(30, TimeUnit.SECONDS);
    Assert.assertEquals("Failed to re-create current session", dupZookeeper.getState(), ZooKeeper.States.CONNECTED);
    dupZookeeper.close();
  }

  private void waitForThriftStop() throws Exception {
    Tests.waitFor("Failed to wait for txService to stop", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return Service.State.RUNNING != txService.thriftRPCServerState();
      }
    });
  }

  // the edit log will block until a countdown latch is decremented to simulate heavy load for TEPHRA-132
  private static class SlowTransactionStorage extends InMemoryTransactionStateStorage {
    @Override
    public TransactionLog createLog(long timestamp) throws IOException {
      return new SlowTransactionLog(timestamp);
    }
  }

  private static class SlowTransactionLog extends InMemoryTransactionStateStorage.InMemoryTransactionLog {
    SlowTransactionLog(long timestamp) {
      super(timestamp);
    }

    @Override
    public void append(TransactionEdit edit) throws IOException {
      try {
        storageWaitLatch.await();
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      super.append(edit);
    }

    @Override
    public void append(List<TransactionEdit> edits) throws IOException {
      try {
        storageWaitLatch.await();
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      super.append(edits);
    }
  }
}
