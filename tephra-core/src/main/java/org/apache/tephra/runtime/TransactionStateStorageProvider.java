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
package org.apache.tephra.runtime;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.tephra.persist.NoOpTransactionStateStorage;
import org.apache.tephra.persist.TransactionStateStorage;

/**
 * A provider for {@link TransactionStateStorage} that provides different
 * {@link TransactionStateStorage} implementation based on configuration.
 */
@Singleton
public final class TransactionStateStorageProvider implements Provider<TransactionStateStorage> {

  private final Configuration cConf;
  private final Provider<TransactionStateStorage> storageProvider;
  private final Provider<NoOpTransactionStateStorage> noOpTransactionStateStorageProvider;

  @Inject
  TransactionStateStorageProvider(Configuration cConf,
                                  Provider<NoOpTransactionStateStorage> noOpTransactionStateStorageProvider,
                                  @Named("persist") Provider<TransactionStateStorage> storageProvider) {
    this.cConf = cConf;
    this.storageProvider = storageProvider;
    this.noOpTransactionStateStorageProvider = noOpTransactionStateStorageProvider;
  }

  @Override
  public TransactionStateStorage get() {
    if (cConf.getBoolean(TxConstants.Manager.CFG_DO_PERSIST, true)) {
      return storageProvider.get();
    } else {
      return noOpTransactionStateStorageProvider.get();
    }
  }
}
