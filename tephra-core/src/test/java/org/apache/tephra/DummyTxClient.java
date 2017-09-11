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

import com.google.inject.Inject;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;

import java.util.Collection;

class DummyTxClient extends InMemoryTxSystemClient {

  boolean failCanCommitOnce = false;
  int failCommits = 0;
  enum CommitState {
    Started, Committed, Aborted, Invalidated
  }
  CommitState state = CommitState.Started;

  @Inject
  DummyTxClient(TransactionManager txmgr) {
    super(txmgr);
  }

  @Override
  public void canCommitOrThrow(Transaction tx, Collection<byte[]> changeIds)
    throws TransactionFailureException {
    if (failCanCommitOnce) {
      failCanCommitOnce = false;
      throw new TransactionConflictException(tx.getTransactionId(), "<unknown>", null);
    } else {
      super.canCommitOrThrow(tx, changeIds);
    }
  }

  @Override
  public void commitOrThrow(Transaction tx)
    throws TransactionFailureException {
    if (failCommits-- > 0) {
      throw new TransactionConflictException(tx.getTransactionId(), "<unknown>", null);
    } else {
      state = CommitState.Committed;
      super.commitOrThrow(tx);
    }
  }

  @Override
  public Transaction startLong() {
    state = CommitState.Started;
    return super.startLong();
  }

  @Override
  public Transaction startShort() {
    state = CommitState.Started;
    return super.startShort();
  }

  @Override
  public Transaction startShort(int timeout) {
    state = CommitState.Started;
    return super.startShort(timeout);
  }

  @Override
  public void abort(Transaction tx) {
    state = CommitState.Aborted;
    super.abort(tx);
  }

  @Override
  public boolean invalidate(long tx) {
    state = CommitState.Invalidated;
    return super.invalidate(tx);
  }
}
