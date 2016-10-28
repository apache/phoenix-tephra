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

package org.apache.tephra.hbase.coprocessor.janitor;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Persist data janitor state into an HBase table.
 */
public class DataJanitorState {
  public static final byte[] FAMILY = {'f'};
  private static final byte[] PRUNE_UPPER_BOUND_COL = {'u'};
  private static final byte[] REGION_KEY_PREFIX = {0x1};

  private final TableSupplier stateTableSupplier;


  public DataJanitorState(TableSupplier stateTableSupplier) {
    this.stateTableSupplier = stateTableSupplier;
  }

  public void savePruneUpperBound(byte[] regionId, long pruneUpperBound) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Put put = new Put(makeRegionKey(regionId));
      put.addColumn(FAMILY, PRUNE_UPPER_BOUND_COL, Bytes.toBytes(pruneUpperBound));
      stateTable.put(put);
    }
  }

  public long getPruneUpperBound(byte[] regionId) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Get get = new Get(makeRegionKey(regionId));
      get.addColumn(FAMILY, PRUNE_UPPER_BOUND_COL);
      byte[] result = stateTable.get(get).getValue(FAMILY, PRUNE_UPPER_BOUND_COL);
      return result == null ? -1 : Bytes.toLong(result);
    }
  }

  private byte[] makeRegionKey(byte[] regionId) {
    return Bytes.add(REGION_KEY_PREFIX, regionId);
  }

  /**
   * Supplies table for persisting state
   */
  public interface TableSupplier {
    Table get() throws IOException;
  }
}
