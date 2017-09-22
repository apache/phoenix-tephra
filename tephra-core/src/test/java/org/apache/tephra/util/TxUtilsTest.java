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

package org.apache.tephra.util;

import org.apache.tephra.Transaction;
import org.apache.tephra.TxConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for {@link TxUtils} utility methods.
 */
public class TxUtilsTest {
  @Test
  public void testMaxVisibleTimestamp() {
    // make sure we don't overflow with MAX_VALUE write pointer
    assertEquals(Long.MAX_VALUE, TxUtils.getMaxVisibleTimestamp(Transaction.ALL_VISIBLE_LATEST));
  }

  @Test
  public void testPruneUpperBound() {
    Transaction tx = new Transaction(100, 100, new long[] {10, 30}, new long[] {80, 90}, 80);
    Assert.assertEquals(30, TxUtils.getPruneUpperBound(tx));

    tx = new Transaction(100, 100, new long[] {10, 95}, new long[] {80, 90}, 80);
    Assert.assertEquals(79, TxUtils.getPruneUpperBound(tx));

    tx = new Transaction(100, 110, new long[] {10}, new long[] {}, Transaction.NO_TX_IN_PROGRESS);
    Assert.assertEquals(10, TxUtils.getPruneUpperBound(tx));

    tx = new Transaction(100, 110, new long[] {}, new long[] {60}, 60);
    Assert.assertEquals(59, TxUtils.getPruneUpperBound(tx));

    tx = new Transaction(100, 110, new long[] {}, new long[] {50}, 50);
    Assert.assertEquals(49, TxUtils.getPruneUpperBound(tx));

    tx = new Transaction(100, 110, new long[] {}, new long[] {}, Transaction.NO_TX_IN_PROGRESS);
    Assert.assertEquals(99, TxUtils.getPruneUpperBound(tx));
  }

  @Test
  public void testTTL() {
    long txIdsPerSecond = 1000 * TxConstants.MAX_TX_PER_MS;
    byte[] family = new byte[] { 'd' };
    long currentTxTimeSeconds = 100;
    Transaction tx = new Transaction(100 * txIdsPerSecond, currentTxTimeSeconds * txIdsPerSecond,
                                     new long[] {10 * txIdsPerSecond, 30 * txIdsPerSecond},
                                     new long[] {80 * txIdsPerSecond, 90 * txIdsPerSecond},
                                     80 * txIdsPerSecond);
    int ttlSeconds = 60;
    Map<byte[], Long> ttlByFamily = Collections.singletonMap(family, ttlSeconds * 1000L);
    // ttl should only be impacted by the current transaction's id, and not by any older, in-progress transactions
    Assert.assertEquals((currentTxTimeSeconds - ttlSeconds) * txIdsPerSecond,
                        TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx));
  }

  @Test
  public void testLargeTTL() {
    long txIdsPerSecond = 1000 * TxConstants.MAX_TX_PER_MS;
    byte[] family = new byte[] { 'd' };
    long currentTxTimeSeconds = 100;
    Transaction tx = new Transaction(100 * txIdsPerSecond, currentTxTimeSeconds * txIdsPerSecond,
                                     new long[] { }, new long[] { }, 100);
    // ~100 years, so that the computed start timestamp is prior to 0 (epoch)
    long ttlSeconds = TimeUnit.DAYS.toSeconds(365 * 100);
    Map<byte[], Long> ttlByFamily = Collections.singletonMap(family, ttlSeconds * 1000L);
    // oldest visible timestamp should be 0, not negative, because HBase timestamps can not be negative
    Assert.assertEquals(0, TxUtils.getOldestVisibleTimestamp(ttlByFamily, tx));
  }
}
