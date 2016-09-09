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

package org.apache.tephra;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.persist.InMemoryTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;
import org.apache.tephra.runtime.DefaultTransactionManagerProvider;
import org.apache.tephra.runtime.TransactionModules;
import org.apache.twill.zookeeper.ZKClientService;

/**
 *
 */
public class TestTransactionManagerProvider extends DefaultTransactionManagerProvider {
  @Inject
  public TestTransactionManagerProvider(Configuration conf, ZKClientService zkClientService) {
    super(conf, zkClientService);
  }

  @Override
  protected Module getTransactionModule() {
    return Modules.override(new TransactionModules().getDistributedModules())
      .with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(TransactionStateStorage.class)
            .to(InMemoryTransactionStateStorage.class).in(Scopes.SINGLETON);
        }
      });
  }
}
