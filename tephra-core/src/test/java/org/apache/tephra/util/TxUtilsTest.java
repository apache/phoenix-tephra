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
import org.junit.Assert;
import org.junit.Test;

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
}
