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


import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.AbstractHBaseTableTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test methods of {@link DataJanitorState}
 */
// TODO: Group all the tests that need HBase mini cluster into a suite, so that we start the mini-cluster only once
public class DataJanitorStateTest extends AbstractHBaseTableTest {

  private TableName pruneStateTable;
  private DataJanitorState dataJanitorState;

  @Before
  public void beforeTest() throws Exception {
    pruneStateTable = TableName.valueOf(conf.get(TxConstants.DataJanitor.PRUNE_STATE_TABLE,
                                                 TxConstants.DataJanitor.DEFAULT_PRUNE_STATE_TABLE));
    HTable table = createTable(pruneStateTable.getName(), new byte[][]{DataJanitorState.FAMILY}, false,
                               // Prune state table is a non-transactional table, hence no transaction co-processor
                               Collections.<String>emptyList());
    table.close();

    dataJanitorState =
      new DataJanitorState(new DataJanitorState.TableSupplier() {
        @Override
        public Table get() throws IOException {
          return testUtil.getConnection().getTable(pruneStateTable);
        }
      });

  }

  @After
  public void afterTest() throws Exception {
    hBaseAdmin.disableTable(pruneStateTable);
    hBaseAdmin.deleteTable(pruneStateTable);
  }

  @Test
  public void testSavePruneUpperBound() throws Exception {
    int max = 20;

    // Nothing should be present in the beginning
    Assert.assertEquals(-1, dataJanitorState.getPruneUpperBoundForRegion(Bytes.toBytes(10L)));

    // Save some region - prune upper bound values
    // We should have values for regions 0, 2, 4, 6, ..., max-2 after this
    for (long i = 0; i < max; i += 2) {
      dataJanitorState.savePruneUpperBoundForRegion(Bytes.toBytes(i), i);
    }

    Assert.assertEquals(10L, dataJanitorState.getPruneUpperBoundForRegion(Bytes.toBytes(10L)));

    // Verify all the saved values
    for (long i = 0; i < max; ++i) {
      long expected = i % 2 == 0 ? i : -1;
      Assert.assertEquals(expected, dataJanitorState.getPruneUpperBoundForRegion(Bytes.toBytes(i)));
    }
    // Regions not present should give -1
    Assert.assertEquals(-1, dataJanitorState.getPruneUpperBoundForRegion(Bytes.toBytes(max + 50L)));
    Assert.assertEquals(-1, dataJanitorState.getPruneUpperBoundForRegion(Bytes.toBytes((max + 10L) * -1)));
    Assert.assertEquals(-1, dataJanitorState.getPruneUpperBoundForRegion(Bytes.toBytes(3L)));

    SortedSet<byte[]> allRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    Map<byte[], Long> expectedMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (long i = 0; i < max; ++i) {
      allRegions.add(Bytes.toBytes(i));
      if (i % 2 == 0) {
        expectedMap.put(Bytes.toBytes(i), i);
      }
    }
    Assert.assertEquals(max / 2, expectedMap.size());
    Assert.assertEquals(expectedMap, dataJanitorState.getPruneUpperBoundForRegions(allRegions));

    SortedSet<byte[]> regions = ImmutableSortedSet.orderedBy(Bytes.BYTES_COMPARATOR)
      .add(Bytes.toBytes((max + 20L) * -1))
      .add(Bytes.toBytes(6L))
      .add(Bytes.toBytes(15L))
      .add(Bytes.toBytes(18L))
      .add(Bytes.toBytes(max + 33L))
      .build();
    expectedMap = ImmutableSortedMap.<byte[], Long>orderedBy(Bytes.BYTES_COMPARATOR)
      .put(Bytes.toBytes(6L), 6L)
      .put(Bytes.toBytes(18L), 18L)
      .build();
    Assert.assertEquals(expectedMap, dataJanitorState.getPruneUpperBoundForRegions(regions));

    // Delete regions that have prune upper bound before 15 and not in set (4, 8)
    ImmutableSortedSet<byte[]> excludeRegions =
      ImmutableSortedSet.orderedBy(Bytes.BYTES_COMPARATOR).add(Bytes.toBytes(4L)).add(Bytes.toBytes(8L)).build();
    dataJanitorState.deletePruneUpperBounds(15, excludeRegions);
    // Regions 0, 2, 6 and 10 should have been deleted now
    expectedMap = ImmutableSortedMap.<byte[], Long>orderedBy(Bytes.BYTES_COMPARATOR)
      .put(Bytes.toBytes(4L), 4L)
      .put(Bytes.toBytes(8L), 8L)
      .put(Bytes.toBytes(16L), 16L)
      .put(Bytes.toBytes(18L), 18L)
      .build();
    Assert.assertEquals(expectedMap, dataJanitorState.getPruneUpperBoundForRegions(allRegions));
  }

  @Test
  public void testSaveRegionTime() throws Exception {
    int maxTime = 100;

    // Nothing should be present in the beginning
    Assert.assertNull(dataJanitorState.getRegionsOnOrBeforeTime(maxTime));

    // Save regions for time
    Map<Long, SortedSet<byte[]>> regionsTime = new TreeMap<>();
    for (long time = 0; time < maxTime; time += 10) {
      SortedSet<byte[]> regions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      for (long region = 0; region < 10; region += 2) {
        regions.add(Bytes.toBytes((time * 10) + region));
      }
      regionsTime.put(time, regions);
      dataJanitorState.saveRegionsForTime(time, regions);
    }

    // Verify saved regions
    Assert.assertEquals(new TimeRegions(30, regionsTime.get(30L)), dataJanitorState.getRegionsOnOrBeforeTime(30));
    Assert.assertEquals(new TimeRegions(20, regionsTime.get(20L)), dataJanitorState.getRegionsOnOrBeforeTime(25));
    Assert.assertEquals(new TimeRegions(30, regionsTime.get(30L)), dataJanitorState.getRegionsOnOrBeforeTime(31));
    Assert.assertEquals(new TimeRegions(90, regionsTime.get(90L)),
                        dataJanitorState.getRegionsOnOrBeforeTime(maxTime + 1000));
    Assert.assertNull(dataJanitorState.getRegionsOnOrBeforeTime(-10));

    // Delete regions saved on or before time 30
    dataJanitorState.deleteAllRegionsOnOrBeforeTime(30);
    // Values on or before time 30 should be deleted
    Assert.assertNull(dataJanitorState.getRegionsOnOrBeforeTime(30));
    Assert.assertNull(dataJanitorState.getRegionsOnOrBeforeTime(25));
    // Values after time 30 should still exist
    Assert.assertEquals(new TimeRegions(40, regionsTime.get(40L)), dataJanitorState.getRegionsOnOrBeforeTime(40));
  }

  @Test
  public void testSaveInactiveTransactionBoundTime() throws Exception {
    int maxTime = 100;

    // Nothing sould be present in the beginning
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime(10));

    // Save inactive transaction bounds for various time values
    for (long time = 0; time < maxTime; time += 10) {
      dataJanitorState.saveInactiveTransactionBoundForTime(time, time + 2);
    }

    // Verify written values
    Assert.assertEquals(2, dataJanitorState.getInactiveTransactionBoundForTime(0));
    Assert.assertEquals(12, dataJanitorState.getInactiveTransactionBoundForTime(10));
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime(15));
    Assert.assertEquals(92, dataJanitorState.getInactiveTransactionBoundForTime(90));
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime(maxTime + 100));
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime((maxTime + 55) * -1L));

    // Delete values saved on or before time 20
    dataJanitorState.deleteInactiveTransactionBoundsOnOrBeforeTime(20);
    // Values on or before time 20 should be deleted
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime(0));
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime(10));
    Assert.assertEquals(-1, dataJanitorState.getInactiveTransactionBoundForTime(20));
    // Values after time 20 should still exist
    Assert.assertEquals(32, dataJanitorState.getInactiveTransactionBoundForTime(30));
    Assert.assertEquals(92, dataJanitorState.getInactiveTransactionBoundForTime(90));
  }
}
