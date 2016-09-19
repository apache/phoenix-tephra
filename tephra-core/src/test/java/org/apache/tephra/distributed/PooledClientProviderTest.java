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

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionServiceMain;
import org.apache.tephra.TxConstants;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionClientModule;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.runtime.ZKModule;
import org.apache.tephra.util.Tests;
import org.apache.thrift.TException;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class PooledClientProviderTest {

  private static final int MAX_CLIENT_COUNT = 3;
  private static final long CLIENT_OBTAIN_TIMEOUT = 10;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testClientConnectionPoolMaximumNumberOfClients() throws Exception {
    // We need a server for the client to connect to
    InMemoryZKServer zkServer = InMemoryZKServer.builder().setDataDir(tmpFolder.newFolder()).build();
    zkServer.startAndWait();

    try {
      Configuration conf = new Configuration();
      conf.set(TxConstants.Service.CFG_DATA_TX_ZOOKEEPER_QUORUM, zkServer.getConnectionStr());
      conf.set(TxConstants.Manager.CFG_TX_SNAPSHOT_DIR, tmpFolder.newFolder().getAbsolutePath());
      conf.set("data.tx.client.count", Integer.toString(MAX_CLIENT_COUNT));
      conf.set("data.tx.client.obtain.timeout", Long.toString(CLIENT_OBTAIN_TIMEOUT));

      final TransactionServiceMain main = new TransactionServiceMain(conf);
      final CountDownLatch latch = new CountDownLatch(1);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            main.start();
            latch.countDown();
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };

      try {
        t.start();
        // Wait for service to startup
        latch.await();

        startClientAndTestPool(conf);
      } finally {
        main.stop();
        t.join();
      }
    } finally {
      zkServer.stopAndWait();
    }
  }

  private void startClientAndTestPool(Configuration conf) throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new ZKModule(),
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );

    ZKClientService zkClient = injector.getInstance(ZKClientService.class);
    zkClient.startAndWait();

    final PooledClientProvider clientProvider = new PooledClientProvider(conf,
      injector.getInstance(DiscoveryServiceClient.class));

    // Test simple case of get + return. Note: this also initializes the provider's pool, which
    // takes about one second (discovery). Doing it before we test the threads makes it so that one
    // thread doesn't take exceptionally longer than the others.

    // Need to retry, since TransactionServiceMain#start returning doesn't indicate that the TransactionService
    // has registered itself for discovery yet
    Tests.waitFor("Failed to get client.", new Callable<Boolean>() {
      @SuppressWarnings({"unused", "EmptyTryBlock"})
      @Override
      public Boolean call() throws Exception {
        try (CloseableThriftClient closeableThriftClient = clientProvider.getCloseableClient()) {
          // do nothing with the client
        } catch (TException e) {
          // simply retry
          return false;
        }
        return true;
      }
    });

    //Now race to get MAX_CLIENT_COUNT+1 clients, exhausting the pool and requesting 1 more.
    List<Future<Integer>> clientIds = new ArrayList<>();
    // We want to ensure that all clients have been exhausted before releasing any.
    // Only once all the clients are fetched from the pool, will any be released. The last thread will reuse one of
    // these clients from the pool.
    CountDownLatch clientDoneLatch = new CountDownLatch(MAX_CLIENT_COUNT);
    ExecutorService executor = Executors.newFixedThreadPool(MAX_CLIENT_COUNT + 1);
    for (int i = 0; i < MAX_CLIENT_COUNT + 1; i++) {
      clientIds.add(executor.submit(new RetrieveClient(clientProvider, clientDoneLatch)));
    }

    Set<Integer> ids = new HashSet<>();
    for (Future<Integer> id : clientIds) {
      ids.add(id.get());
    }
    Assert.assertEquals(MAX_CLIENT_COUNT, ids.size());

    // Now, try it again with, with a countdown latch equal to the number of threads. All of them will only progress
    // past it, once they all acquire a client or time out while attempting to obtain one.
    // One of the threads should throw a TimeOutException, because the other threads don't release their clients
    // until then and the client thread pool isn't enough for the number of threads.
    clientDoneLatch = new CountDownLatch(MAX_CLIENT_COUNT + 1);
    for (int i = 0; i < MAX_CLIENT_COUNT + 1; i++) {
      clientIds.add(executor.submit(new RetrieveClient(clientProvider, clientDoneLatch)));
    }
    int numTimeoutExceptions = 0;
    for (Future<Integer> clientId : clientIds) {
      try {
        clientId.get();
      } catch (ExecutionException expected) {
        Assert.assertEquals(TimeoutException.class, expected.getCause().getClass());
        numTimeoutExceptions++;
      }
    }
    // expect that exactly one of the threads hit the TimeoutException
    Assert.assertEquals(String.format("Expected one thread to not obtain a client within %s milliseconds.",
                                      CLIENT_OBTAIN_TIMEOUT),
                        1, numTimeoutExceptions);

    executor.shutdownNow();
  }

  private static class RetrieveClient implements Callable<Integer> {
    private final PooledClientProvider pool;
    private final CountDownLatch done;

    RetrieveClient(PooledClientProvider pool, CountDownLatch done) {
      this.pool = pool;
      this.done = done;
    }

    @Override
    public Integer call() throws Exception {
      try (CloseableThriftClient client = pool.getCloseableClient()) {
        int id = System.identityHashCode(client.getThriftClient());
        done.countDown();
        done.await();
        return id;
      } catch (TimeoutException e) {
        done.countDown();
        throw e;
      }
    }
  }
}
