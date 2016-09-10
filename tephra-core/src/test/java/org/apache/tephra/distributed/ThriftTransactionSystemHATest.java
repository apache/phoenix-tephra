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

package org.apache.tephra.distributed;

import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.runtime.ConfigModule;
import org.apache.tephra.runtime.DiscoveryModules;
import org.apache.tephra.runtime.TransactionClientModule;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.tephra.util.Tests;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.Callable;

/**
 * Test HA behavior of Transaction Service
 */
public class ThriftTransactionSystemHATest extends ThriftTransactionSystemTest {

  @BeforeClass
  public static void start() throws Exception {
    // Start tx service
    ThriftTransactionSystemTest.start();

    // Expire zk session to make tx service follower
    Tests.expireZkSession(zkClientService);
    Tests.waitFor("Failed to wait for txService to stop", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return Service.State.RUNNING != txService.thriftRPCServerState();
      }
    });

    // wait for the thrift rpc server to be in running state again
    Tests.waitFor("Failed to wait for txService to be running.", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return Service.State.RUNNING == txService.thriftRPCServerState();
      }
    });

    // we need to get a new txClient, because the old one will no longer work after the thrift server restart
    Injector injector = Guice.createInjector(
      new ConfigModule(conf),
      new AbstractModule() {
        @Override
        protected void configure() {
          // Instead of using ZKClientModule that will create new instance of ZKClient, we create instance
          // binding to reuse the same ZKClient used for leader election
          bind(ZKClient.class).toInstance(zkClientService);
          bind(ZKClientService.class).toInstance(zkClientService);
        }
      },
      new DiscoveryModules().getDistributedModules(),
      new TransactionModules().getDistributedModules(),
      new TransactionClientModule()
    );
    txClient = injector.getInstance(TransactionSystemClient.class);
    Tests.waitForTxReady(txClient);
  }

  @AfterClass
  public static void stop() throws Exception {
    ThriftTransactionSystemTest.stop();
  }
}
