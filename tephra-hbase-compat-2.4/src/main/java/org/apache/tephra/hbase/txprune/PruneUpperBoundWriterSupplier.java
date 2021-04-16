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

package org.apache.tephra.hbase.txprune;


import com.google.common.base.Supplier;
import org.apache.hadoop.hbase.TableName;
import org.apache.tephra.coprocessor.CacheSupplier;
import org.apache.tephra.coprocessor.ReferenceCountedSupplier;

/**
 * Supplies instances of {@link PruneUpperBoundWriter} implementations.
 */
public class PruneUpperBoundWriterSupplier implements CacheSupplier<PruneUpperBoundWriter> {

  private static final ReferenceCountedSupplier<PruneUpperBoundWriter> referenceCountedSupplier =
    new ReferenceCountedSupplier<>(PruneUpperBoundWriter.class.getSimpleName());

  private final Supplier<PruneUpperBoundWriter> supplier;

  public PruneUpperBoundWriterSupplier(final TableName tableName, final DataJanitorState dataJanitorState,
                                       final long pruneFlushInterval) {
    this.supplier = new Supplier<PruneUpperBoundWriter>() {
      @Override
      public PruneUpperBoundWriter get() {
        return new PruneUpperBoundWriter(tableName, dataJanitorState, pruneFlushInterval);
      }
    };
  }

  @Override
  public PruneUpperBoundWriter get() {
    return referenceCountedSupplier.getOrCreate(supplier);
  }

  public void release() {
    referenceCountedSupplier.release();
  }
}
