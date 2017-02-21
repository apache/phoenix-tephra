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

package org.apache.tephra.manager;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class InvalidTxListTest {

  @Test
  public void testInvalidTxList() {
    InvalidTxList invalidTxList = new InvalidTxList();
    // Assert that the list is empty at the beginning
    Assert.assertTrue(invalidTxList.isEmpty());
    Assert.assertEquals(0, invalidTxList.size());
    Assert.assertEquals(ImmutableList.of(), invalidTxList.toRawList());
    Assert.assertArrayEquals(new long[0], invalidTxList.toSortedArray());

    // Try removing something from the empty list
    Assert.assertFalse(invalidTxList.remove(3));
    Assert.assertFalse(invalidTxList.removeAll(ImmutableList.of(5L, 9L)));

    // verify contains
    Assert.assertFalse(invalidTxList.contains(3));

    // Add some elements to the list
    invalidTxList.add(3);
    invalidTxList.add(1);
    invalidTxList.add(8);
    invalidTxList.add(5);

    // verify contains
    Assert.assertTrue(invalidTxList.contains(3));

    // Assert the newly added elements
    Assert.assertFalse(invalidTxList.isEmpty());
    Assert.assertEquals(4, invalidTxList.size());
    Assert.assertEquals(ImmutableList.of(3L, 1L, 8L, 5L), invalidTxList.toRawList());
    Assert.assertArrayEquals(new long[] {1, 3, 5, 8}, invalidTxList.toSortedArray());

    // Add a collection of elements
    invalidTxList.addAll(ImmutableList.of(7L, 10L, 4L, 2L));

    // Assert the newly added elements
    Assert.assertFalse(invalidTxList.isEmpty());
    Assert.assertEquals(8, invalidTxList.size());
    Assert.assertEquals(ImmutableList.of(3L, 1L, 8L, 5L, 7L, 10L, 4L, 2L), invalidTxList.toRawList());
    Assert.assertArrayEquals(new long[] {1, 2, 3, 4, 5, 7, 8, 10}, invalidTxList.toSortedArray());

    // Remove elements that are not present
    Assert.assertFalse(invalidTxList.remove(6));
    Assert.assertFalse(invalidTxList.removeAll(ImmutableList.of(9L, 11L)));

    // Remove a collection of elements
    Assert.assertTrue(invalidTxList.removeAll(ImmutableList.of(8L, 4L, 2L)));
    // This time check the array first and then check the list
    Assert.assertArrayEquals(new long[] {1, 3, 5, 7, 10}, invalidTxList.toSortedArray());
    Assert.assertEquals(ImmutableList.of(3L, 1L, 5L, 7L, 10L), invalidTxList.toRawList());

    // Remove a single element
    Assert.assertTrue(invalidTxList.remove(5));
    Assert.assertArrayEquals(new long[] {1, 3, 7, 10}, invalidTxList.toSortedArray());
    Assert.assertEquals(ImmutableList.of(3L, 1L, 7L, 10L), invalidTxList.toRawList());

    // Add a LongCollection
    invalidTxList.addAll(new LongArrayList(new long[] {15, 12, 13}));

    // Assert the newly added elements
    Assert.assertEquals(7, invalidTxList.size());
    Assert.assertArrayEquals(new long[] {1, 3, 7, 10, 12, 13, 15}, invalidTxList.toSortedArray());
    Assert.assertEquals(ImmutableList.of(3L, 1L, 7L, 10L, 15L, 12L, 13L), invalidTxList.toRawList());

    // Remove a LongCollection
    invalidTxList.removeAll(new LongArrayList(new long[] {3, 7, 12}));

    // Assert removals
    Assert.assertEquals(4, invalidTxList.size());
    Assert.assertArrayEquals(new long[] {1, 10, 13, 15}, invalidTxList.toSortedArray());
    Assert.assertEquals(ImmutableList.of(1L, 10L, 15L, 13L), invalidTxList.toRawList());

    // Clear the list
    invalidTxList.clear();
    Assert.assertTrue(invalidTxList.isEmpty());
    Assert.assertEquals(0, invalidTxList.size());
    Assert.assertArrayEquals(new long[0], invalidTxList.toSortedArray());
    Assert.assertEquals(ImmutableList.of(), invalidTxList.toRawList());
  }
}
