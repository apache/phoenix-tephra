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

/**
 * Thrown to indicate transaction conflict occurred when trying to commit a transaction.
 */
public class TransactionConflictException extends TransactionFailureException {

  private final Long transactionId;
  private final String conflictingKey;
  private final String conflictingClient;

  /**
   * @deprecated since 0.13-incubating. Use {@link #TransactionConflictException(long, String, String)} instead.
   */
  @Deprecated
  public TransactionConflictException(String message) {
    super(message);
    transactionId = null;
    conflictingKey = null;
    conflictingClient = null;
  }

  /**
   * @deprecated since 0.13-incubating. Use {@link #TransactionConflictException(long, String, String)} instead.
   */
  @Deprecated
  public TransactionConflictException(String message, Throwable cause) {
    super(message, cause);
    transactionId = null;
    conflictingKey = null;
    conflictingClient = null;
  }

  public TransactionConflictException(long transactionId, String conflictingKey, String conflictingClient) {
    super(String.format("Transaction %d conflicts with %s on change key '%s'", transactionId,
                        conflictingClient == null ? "unknown client" : conflictingClient, conflictingKey));
    this.transactionId = transactionId;
    this.conflictingKey = conflictingKey;
    this.conflictingClient = conflictingClient;
  }

  public Long getTransactionId() {
    return transactionId;
  }

  public String getConflictingKey() {
    return conflictingKey;
  }

  public String getConflictingClient() {
    return conflictingClient;
  }
}
