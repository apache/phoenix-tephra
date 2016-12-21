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

package org.apache.tephra.hbase.txprune;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.tephra.Transaction;
import org.apache.tephra.persist.TransactionVisibilityState;
import org.apache.tephra.util.TxUtils;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Record compaction state for invalid list pruning
 */
public class CompactionState {
  private static final Log LOG = LogFactory.getLog(CompactionState.class);

  private final byte[] regionName;
  private final String regionNameAsString;
  private final TableName stateTable;
  private final long txMaxLifetimeMills;
  private final DataJanitorState dataJanitorState;
  private volatile long pruneUpperBound = -1;

  public CompactionState(final RegionCoprocessorEnvironment env, final TableName stateTable, long txMaxLifetimeMills) {
    this.regionName = env.getRegionInfo().getRegionName();
    this.regionNameAsString = env.getRegionInfo().getRegionNameAsString();
    this.stateTable = stateTable;
    this.txMaxLifetimeMills = txMaxLifetimeMills;
    this.dataJanitorState = new DataJanitorState(new DataJanitorState.TableSupplier() {
      @Override
      public Table get() throws IOException {
        return env.getTable(stateTable);
      }
    });
  }

  /**
   * Records the transaction state used for a compaction. This method is called when the compaction starts.
   *
   * @param request {@link CompactionRequest} for the compaction
   * @param snapshot transaction state that will be used for the compaction
   */
  public void record(CompactionRequest request, @Nullable TransactionVisibilityState snapshot) {
    if (request.isMajor() && snapshot != null) {
      Transaction tx = TxUtils.createDummyTransaction(snapshot);
      pruneUpperBound = TxUtils.getPruneUpperBound(tx);
      LOG.debug(
        String.format("Computed prune upper bound %s for compaction request %s using transaction state from time %s",
                      pruneUpperBound, request, snapshot.getTimestamp()));
    } else {
      pruneUpperBound = -1;
    }
  }

  /**
   * Persists the transaction state recorded by {@link #record(CompactionRequest, TransactionVisibilityState)}.
   * This method is called after the compaction has successfully completed.
   */
  public void persist() {
    if (pruneUpperBound != -1) {
      try {
        dataJanitorState.savePruneUpperBoundForRegion(regionName, pruneUpperBound);
        LOG.debug(String.format("Saved prune upper bound %s for region %s", pruneUpperBound, regionNameAsString));
      } catch (IOException e) {
        LOG.warn(String.format("Cannot record prune upper bound in table %s after compacting region %s",
                               stateTable, regionNameAsString), e);
      }
    }
  }
}
