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

package org.apache.tephra.util;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.TxConstants;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Common methods used by Tephra tests.
 */
public final class Tests {

  private Tests() {}

  public static void waitFor(String errorMessage, Callable<Boolean> callable) throws Exception {
    for (int i = 0; i < 600; i++) {
      if (callable.call()) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    Assert.fail(errorMessage);
  }

  public static void waitForTxReady(final TransactionSystemClient txClient) throws Exception {
    waitFor("Timeout waiting for transaction manager to be running", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          String status = txClient.status();
          return TxConstants.STATUS_OK.equals(status);
        } catch (Exception e) {
          return false;
        }
      }
    });
  }

  public static void expireZkSession(ZKClientService zkClientService) throws Exception {
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
}
