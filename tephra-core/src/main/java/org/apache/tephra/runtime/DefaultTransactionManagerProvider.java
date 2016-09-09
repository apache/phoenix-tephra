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

package org.apache.tephra.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * A provider for {@link TransactionManager} that provides a new instance every time.
 */
public class DefaultTransactionManagerProvider implements Provider<TransactionManager> {
  private final Configuration conf;
  private final ZKClientService zkClientService;

  @Inject
  public DefaultTransactionManagerProvider(Configuration conf, ZKClientService zkClientService) {
    this.conf = conf;
    this.zkClientService = zkClientService;
  }

  @Override
  public TransactionManager get() {
    // Create a new injector every time since Guice services cannot be restarted TEPHRA-179
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
      new TransactionClientModule(),
      getTransactionModule()
    );
    return injector.getInstance(TransactionManager.class);
  }

  @VisibleForTesting
  protected Module getTransactionModule() {
    return new TransactionModules().getDistributedModules();
  }
}
