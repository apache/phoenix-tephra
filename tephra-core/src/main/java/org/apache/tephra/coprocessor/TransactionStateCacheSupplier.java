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

package org.apache.tephra.coprocessor;

import com.google.common.base.Supplier;
import org.apache.hadoop.conf.Configuration;

/**
 * Supplies instances of {@link TransactionStateCache} implementations.
 */
public class TransactionStateCacheSupplier implements CacheSupplier<TransactionStateCache> {

  private static final ReferenceCountedSupplier<TransactionStateCache> referenceCountedSupplier =
    new ReferenceCountedSupplier<>(TransactionStateCache.class.getSimpleName());

  private final Supplier<TransactionStateCache> supplier;

  public TransactionStateCacheSupplier(Supplier<TransactionStateCache> supplier) {
    this.supplier = supplier;
  }

  public TransactionStateCacheSupplier(final Configuration conf) {
    this.supplier = new Supplier<TransactionStateCache>() {
      @Override
      public TransactionStateCache get() {
        TransactionStateCache transactionStateCache = new TransactionStateCache();
        transactionStateCache.setConf(conf);
        return transactionStateCache;
      }
    };
  }

  /**
   * Returns a singleton instance of the transaction state cache, performing lazy initialization if necessary.
   *
   * @return A shared instance of the transaction state cache
   */
  @Override
  public TransactionStateCache get() {
    return referenceCountedSupplier.getOrCreate(supplier);
  }

  @Override
  public void release() {
    referenceCountedSupplier.release();
  }
}
