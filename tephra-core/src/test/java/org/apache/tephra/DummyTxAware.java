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

package org.apache.tephra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

class DummyTxAware implements TransactionAware {

  enum InduceFailure { NoFailure, ReturnFalse, ThrowException }

  boolean started = false;
  boolean committed = false;
  boolean checked = false;
  boolean rolledBack = false;
  boolean postCommitted = false;
  private List<byte[]> changes = Lists.newArrayList();

  InduceFailure failStartTxOnce = InduceFailure.NoFailure;
  InduceFailure failChangesTxOnce = InduceFailure.NoFailure;
  InduceFailure failCommitTxOnce = InduceFailure.NoFailure;
  InduceFailure failPostCommitTxOnce = InduceFailure.NoFailure;
  InduceFailure failRollbackTxOnce = InduceFailure.NoFailure;

  void addChange(byte[] key) {
    changes.add(key);
  }

  void reset() {
    started = false;
    checked = false;
    committed = false;
    rolledBack = false;
    postCommitted = false;
    changes.clear();
  }

  @Override
  public void startTx(Transaction tx) {
    reset();
    started = true;
    if (failStartTxOnce == InduceFailure.ThrowException) {
      failStartTxOnce = InduceFailure.NoFailure;
      throw new RuntimeException("start failure");
    }
  }

  @Override
  public void updateTx(Transaction tx) {
    // do nothing
  }

  @Override
  public Collection<byte[]> getTxChanges() {
    checked = true;
    if (failChangesTxOnce == InduceFailure.ThrowException) {
      failChangesTxOnce = InduceFailure.NoFailure;
      throw new RuntimeException("changes failure");
    }
    return ImmutableList.copyOf(changes);
  }

  @Override
  public boolean commitTx() throws Exception {
    committed = true;
    if (failCommitTxOnce == InduceFailure.ThrowException) {
      failCommitTxOnce = InduceFailure.NoFailure;
      throw new RuntimeException("persist failure");
    }
    if (failCommitTxOnce == InduceFailure.ReturnFalse) {
      failCommitTxOnce = InduceFailure.NoFailure;
      return false;
    }
    return true;
  }

  @Override
  public void postTxCommit() {
    postCommitted = true;
    if (failPostCommitTxOnce == InduceFailure.ThrowException) {
      failPostCommitTxOnce = InduceFailure.NoFailure;
      throw new RuntimeException("post failure");
    }
  }

  @Override
  public boolean rollbackTx() throws Exception {
    rolledBack = true;
    if (failRollbackTxOnce == InduceFailure.ThrowException) {
      failRollbackTxOnce = InduceFailure.NoFailure;
      throw new RuntimeException("rollback failure");
    }
    if (failRollbackTxOnce == InduceFailure.ReturnFalse) {
      failRollbackTxOnce = InduceFailure.NoFailure;
      return false;
    }
    return true;
  }

  @Override
  public String getTransactionAwareName() {
    return "dummy";
  }
}
