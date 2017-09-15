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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.AbstractHBaseTableTest;
import org.apache.tephra.txprune.RegionPruneInfo;
import org.apache.tephra.txprune.hbase.RegionsAtTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Test {@link InvalidListPruningDebugTool}.
 */
public class InvalidListPruningDebugTest extends AbstractHBaseTableTest {
  private static final Gson GSON = new Gson();
  private static final boolean DEBUG_PRINT = true;
  private static final Function<RegionPruneInfo, byte[]> PRUNE_INFO_TO_BYTES =
    new Function<RegionPruneInfo, byte[]>() {
      @Override
      public byte[] apply(RegionPruneInfo input) {
        return input.getRegionName();
      }
    };
  private static final Function<RegionPruneInfo, String> PRUNE_INFO_TO_STRING =
    new Function<RegionPruneInfo, String>() {
      @Override
      public String apply(RegionPruneInfo input) {
        return input.getRegionNameAsString();
      }
    };
  private static final Function<String, byte[]> STRING_TO_BYTES =
    new Function<String, byte[]>() {
      @Override
      public byte[] apply(String input) {
        return Bytes.toBytes(input);
      }
    };
  private static final Type PRUNE_INFO_LIST_TYPE =
    new TypeToken<List<InvalidListPruningDebugTool.RegionPruneInfoPretty>>() { }.getType();

  private static TableName pruneStateTable;
  private static InvalidListPruningDebugTool pruningDebug;

  private static TreeMultimap<Long, InvalidListPruningDebugTool.RegionPruneInfoPretty> compactedRegions =
    TreeMultimap.create(Ordering.<Long>natural(), stringComparator());
  private static TreeMultimap<Long, String> emptyRegions = TreeMultimap.create();
  private static TreeMultimap<Long, String> notCompactedRegions = TreeMultimap.create();
  private static TreeMultimap<Long, InvalidListPruningDebugTool.RegionPruneInfoPretty> deletedRegions =
    TreeMultimap.create(Ordering.<Long>natural(), stringComparator());


  @BeforeClass
  public static void addData() throws Exception {
    pruneStateTable = TableName.valueOf(conf.get(TxConstants.TransactionPruning.PRUNE_STATE_TABLE,
                                                 TxConstants.TransactionPruning.DEFAULT_PRUNE_STATE_TABLE));
    HTable table = createTable(pruneStateTable.getName(), new byte[][]{DataJanitorState.FAMILY}, false,
                               // Prune state table is a non-transactional table, hence no transaction co-processor
                               Collections.<String>emptyList());
    table.close();

    DataJanitorState dataJanitorState =
      new DataJanitorState(new DataJanitorState.TableSupplier() {
        @Override
        public Table get() throws IOException {
          return testUtil.getConnection().getTable(pruneStateTable);
        }
      });

    // Record prune upper bounds for 9 regions
    long now = System.currentTimeMillis();
    int maxRegions = 9;
    TableName compactedTable = TableName.valueOf("default", "compacted_table");
    TableName emptyTable = TableName.valueOf("default", "empty_table");
    TableName notCompactedTable = TableName.valueOf("default", "not_compacted_table");
    TableName deletedTable = TableName.valueOf("default", "deleted_table");
    for (long i = 0; i < maxRegions; ++i) {
      // Compacted region
      byte[] compactedRegion = HRegionInfo.createRegionName(compactedTable, null, i, true);
      // The first three regions are recorded at one time, second set at another and the third set at a different time
      long recordTime = now - 6000 + (i / 3) * 100;
      long pruneUpperBound = (now - (i / 3) * 100000) * TxConstants.MAX_TX_PER_MS;
      dataJanitorState.savePruneUpperBoundForRegion(compactedRegion, pruneUpperBound);
      RegionPruneInfo pruneInfo = dataJanitorState.getPruneInfoForRegion(compactedRegion);
      compactedRegions.put(recordTime, new InvalidListPruningDebugTool.RegionPruneInfoPretty(pruneInfo));

      // Empty region
      byte[] emptyRegion = HRegionInfo.createRegionName(emptyTable, null, i, true);
      dataJanitorState.saveEmptyRegionForTime(recordTime + 1, emptyRegion);
      emptyRegions.put(recordTime, Bytes.toString(emptyRegion));

      // Not compacted region
      byte[] notCompactedRegion = HRegionInfo.createRegionName(notCompactedTable, null, i, true);
      notCompactedRegions.put(recordTime, Bytes.toString(notCompactedRegion));

      // Deleted region
      byte[] deletedRegion = HRegionInfo.createRegionName(deletedTable, null, i, true);
      dataJanitorState.savePruneUpperBoundForRegion(deletedRegion, pruneUpperBound - 1000);
      RegionPruneInfo deletedPruneInfo = dataJanitorState.getPruneInfoForRegion(deletedRegion);
      deletedRegions.put(recordTime, new InvalidListPruningDebugTool.RegionPruneInfoPretty(deletedPruneInfo));
    }

    // Also record some common regions across all runs
    byte[] commonCompactedRegion =
      HRegionInfo.createRegionName(TableName.valueOf("default:common_compacted"), null, 100, true);
    byte[] commonNotCompactedRegion =
      HRegionInfo.createRegionName(TableName.valueOf("default:common_not_compacted"), null, 100, true);
    byte[] commonEmptyRegion =
      HRegionInfo.createRegionName(TableName.valueOf("default:common_empty"), null, 100, true);
    // Create one region that is the latest deleted region, this region represents a region that gets recorded
    // every prune run, but gets deleted just before the latest run.
    byte[] newestDeletedRegion =
      HRegionInfo.createRegionName(TableName.valueOf("default:newest_deleted"), null, 100, true);

    int runs = maxRegions / 3;
    for (int i = 0; i < runs; ++i) {
      long recordTime = now - 6000 + i * 100;
      long pruneUpperBound = (now - i * 100000) * TxConstants.MAX_TX_PER_MS;

      dataJanitorState.savePruneUpperBoundForRegion(commonCompactedRegion, pruneUpperBound - 1000);
      RegionPruneInfo c = dataJanitorState.getPruneInfoForRegion(commonCompactedRegion);
      compactedRegions.put(recordTime, new InvalidListPruningDebugTool.RegionPruneInfoPretty(c));

      dataJanitorState.saveEmptyRegionForTime(recordTime + 1, commonEmptyRegion);
      emptyRegions.put(recordTime, Bytes.toString(commonEmptyRegion));

      notCompactedRegions.put(recordTime, Bytes.toString(commonNotCompactedRegion));

      // Record the latest deleted region in all the runs except the last one
      if (i < runs - 1) {
        dataJanitorState.savePruneUpperBoundForRegion(newestDeletedRegion, pruneUpperBound - 1000);
        RegionPruneInfo d = dataJanitorState.getPruneInfoForRegion(newestDeletedRegion);
        compactedRegions.put(recordTime, new InvalidListPruningDebugTool.RegionPruneInfoPretty(d));
      }
    }

    // Record the regions present at various times
    for (long time : compactedRegions.asMap().keySet()) {
      Set<byte[]> allRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      Iterables.addAll(allRegions, Iterables.transform(compactedRegions.get(time), PRUNE_INFO_TO_BYTES));
      Iterables.addAll(allRegions, Iterables.transform(emptyRegions.get(time), STRING_TO_BYTES));
      Iterables.addAll(allRegions, Iterables.transform(notCompactedRegions.get(time), STRING_TO_BYTES));
      dataJanitorState.saveRegionsForTime(time, allRegions);
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    pruningDebug.destroy();

    hBaseAdmin.disableTable(pruneStateTable);
    hBaseAdmin.deleteTable(pruneStateTable);
  }

  @Before
  public void before() throws Exception {
    pruningDebug = new InvalidListPruningDebugTool();
    pruningDebug.initialize(conf);
  }

  @After
  public void after() throws Exception {
    pruningDebug.destroy();
  }

  @Test
  public void testUsage() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(outputStream)) {
      Assert.assertFalse(pruningDebug.execute(new String[0], out));
      out.flush();
      readOutputStream(outputStream);
    }
  }

  @Test
  public void testTimeRegions() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(outputStream)) {
      // Get the latest regions for latest recorded time
      Long latestRecordTime = compactedRegions.asMap().lastKey();
      Assert.assertTrue(pruningDebug.execute(new String[] {"time-region"}, out));
      out.flush();
      Assert.assertEquals(GSON.toJson(expectedRegionsForTime(latestRecordTime)), readOutputStream(outputStream));

      // Get the latest regions for latest recorded time by giving the timestamp
      long now = System.currentTimeMillis();
      outputStream.reset();
      Assert.assertTrue(pruningDebug.execute(new String[] {"time-region", Long.toString(now)}, out));
      out.flush();
      Assert.assertEquals(GSON.toJson(expectedRegionsForTime(latestRecordTime)), readOutputStream(outputStream));

      // Using relative time
      outputStream.reset();
      Assert.assertTrue(pruningDebug.execute(new String[] {"time-region", "now-1s"}, out));
      out.flush();
      Assert.assertEquals(GSON.toJson(expectedRegionsForTime(latestRecordTime)), readOutputStream(outputStream));

      // Get the regions for the oldest recorded time
      Long oldestRecordTime = compactedRegions.asMap().firstKey();
      outputStream.reset();
      Assert.assertTrue(pruningDebug.execute(new String[] {"time-region", Long.toString(oldestRecordTime)}, out));
      out.flush();
      Assert.assertEquals(GSON.toJson(expectedRegionsForTime(oldestRecordTime)), readOutputStream(outputStream));
    }
  }

  @Test
  public void testGetPruneInfo() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(outputStream)) {
      Long recordTime = compactedRegions.asMap().lastKey();
      RegionPruneInfo pruneInfo = compactedRegions.get(recordTime).first();
      Assert.assertTrue(pruningDebug.execute(new String[]{"prune-info", pruneInfo.getRegionNameAsString()}, out));
      out.flush();
      Assert.assertEquals(GSON.toJson(new InvalidListPruningDebugTool.RegionPruneInfoPretty(pruneInfo)),
                          readOutputStream(outputStream));

      // non-exising region
      String nonExistingRegion = "non-existing-region";
      outputStream.reset();
      Assert.assertTrue(pruningDebug.execute(new String[]{"prune-info", nonExistingRegion}, out));
      out.flush();
      Assert.assertEquals(String.format("No prune info found for the region %s.", nonExistingRegion),
                          readOutputStream(outputStream));
    }
  }

  @Test
  public void testIdleRegions() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(outputStream)) {
      // Get the list of regions that have the lowest prune upper bounds for the latest record time
      Long latestRecordTime = compactedRegions.asMap().lastKey();
      SortedSet<InvalidListPruningDebugTool.RegionPruneInfoPretty> latestExpected =
        ImmutableSortedSet.copyOf(pruneUpperBoundAndStringComparator(), compactedRegions.get(latestRecordTime));
      pruningDebug.execute(new String[]{"idle-regions", "-1"}, out);
      out.flush();
      assertEquals(latestExpected, readOutputStream(outputStream));

      // Same command with explicit time
      outputStream.reset();
      pruningDebug.execute(new String[]{"idle-regions", "-1", String.valueOf(latestRecordTime)}, out);
      out.flush();
      assertEquals(latestExpected, readOutputStream(outputStream));

      // Same command with relative time
      outputStream.reset();
      pruningDebug.execute(new String[]{"idle-regions", "-1", "now-2s"}, out);
      out.flush();
      assertEquals(latestExpected, readOutputStream(outputStream));

      // Same command with reduced number of regions
      outputStream.reset();
      int limit = 2;
      pruningDebug.execute(new String[]{"idle-regions", String.valueOf(limit), String.valueOf(latestRecordTime)}, out);
      out.flush();
      Assert.assertEquals(GSON.toJson(subset(latestExpected, 0, limit)), readOutputStream(outputStream));

      // For a different time, this time only live regions that are compacted are returned
      outputStream.reset();
      Long secondLastRecordTime = Iterables.get(compactedRegions.keySet(), 1);
      Set<String> compactedRegionsTime =
        Sets.newTreeSet(Iterables.transform(compactedRegions.get(secondLastRecordTime), PRUNE_INFO_TO_STRING));
      Set<String> compactedRegionsLatest =
        Sets.newTreeSet(Iterables.transform(compactedRegions.get(latestRecordTime), PRUNE_INFO_TO_STRING));
      Set<String> liveExpected = new TreeSet<>(Sets.intersection(compactedRegionsTime, compactedRegionsLatest));
      pruningDebug.execute(new String[]{"idle-regions", "-1", String.valueOf(latestRecordTime - 1)}, out);
      out.flush();
      List<RegionPruneInfo> actual = GSON.fromJson(readOutputStream(outputStream), PRUNE_INFO_LIST_TYPE);
      Assert.assertEquals(liveExpected, Sets.newTreeSet(Iterables.transform(actual, PRUNE_INFO_TO_STRING)));
    }
  }

  @Test
  public void testToCompactRegions() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try (PrintWriter out = new PrintWriter(outputStream)) {
      // Get the regions that are not compacted for the latest time
      Long latestRecordTime = compactedRegions.asMap().lastKey();
      SortedSet<String> expected = notCompactedRegions.get(latestRecordTime);
      pruningDebug.execute(new String[]{"to-compact-regions", "-1"}, out);
      out.flush();
      Assert.assertEquals(expected, GSON.fromJson(readOutputStream(outputStream), SortedSet.class));

      // Same command with explicit time
      outputStream.reset();
      pruningDebug.execute(new String[]{"to-compact-regions", "-1", String.valueOf(latestRecordTime)}, out);
      out.flush();
      Assert.assertEquals(expected, GSON.fromJson(readOutputStream(outputStream), SortedSet.class));

      // Same command with relative time
      outputStream.reset();
      pruningDebug.execute(new String[]{"to-compact-regions", "-1", "now+1h-3m"}, out);
      out.flush();
      Assert.assertEquals(expected, GSON.fromJson(readOutputStream(outputStream), SortedSet.class));

      // Same command with reduced number of regions
      int limit = 2;
      outputStream.reset();
      pruningDebug.execute(new String[]{"to-compact-regions", String.valueOf(limit), String.valueOf(latestRecordTime)},
                           out);
      out.flush();
      // Assert that the actual set is a subset of expected, with size 2 (since the output is not sorted)
      SortedSet<String> actual = GSON.fromJson(readOutputStream(outputStream),
                                               new TypeToken<SortedSet<String>>() { }.getType());
      Assert.assertEquals(limit, actual.size());
      Assert.assertTrue(Sets.difference(actual, expected).isEmpty());

      // For a different time, only live regions that are not compacted are returned
      outputStream.reset();
      Long secondLastRecordTime = Iterables.get(compactedRegions.keySet(), 1);
      Set<String> compactedRegionsTime = notCompactedRegions.get(secondLastRecordTime);
      Set<String> compactedRegionsLatest = notCompactedRegions.get(latestRecordTime);
      Set<String> liveExpected = new TreeSet<>(Sets.intersection(compactedRegionsTime, compactedRegionsLatest));
      pruningDebug.execute(new String[]{"to-compact-regions", "-1", String.valueOf(secondLastRecordTime)}, out);
      out.flush();
      Assert.assertEquals(GSON.toJson(liveExpected), readOutputStream(outputStream));
    }
  }

  private static RegionsAtTime expectedRegionsForTime(long time) {
    SortedSet<String> regions = new TreeSet<>();
    regions.addAll(Sets.newTreeSet(Iterables.transform(compactedRegions.get(time), PRUNE_INFO_TO_STRING)));
    regions.addAll(emptyRegions.get(time));
    regions.addAll(notCompactedRegions.get(time));
    return new RegionsAtTime(time, regions, new SimpleDateFormat(InvalidListPruningDebugTool.DATE_FORMAT));
  }

  private static Comparator<InvalidListPruningDebugTool.RegionPruneInfoPretty> stringComparator() {
    return new Comparator<InvalidListPruningDebugTool.RegionPruneInfoPretty>() {
      @Override
      public int compare(InvalidListPruningDebugTool.RegionPruneInfoPretty o1,
                         InvalidListPruningDebugTool.RegionPruneInfoPretty o2) {
        return o1.getRegionNameAsString().compareTo(o2.getRegionNameAsString());
      }
    };
  }

  private static Comparator<RegionPruneInfo> pruneUpperBoundAndStringComparator() {
    return new Comparator<RegionPruneInfo>() {
      @Override
      public int compare(RegionPruneInfo o1, RegionPruneInfo o2) {
        int result = Long.compare(o1.getPruneUpperBound(), o2.getPruneUpperBound());
        if (result == 0) {
          return o1.getRegionNameAsString().compareTo(o2.getRegionNameAsString());
        }
        return result;
      }
    };
  }

  private String readOutputStream(ByteArrayOutputStream out) throws UnsupportedEncodingException {
    String s = out.toString(Charsets.UTF_8.toString());
    if (DEBUG_PRINT) {
      System.out.println(s);
    }
    // remove the last newline
    return s.length() <= 1 ? "" : s.substring(0, s.length() - 1);
  }

  private void assertEquals(Collection<? extends RegionPruneInfo> expectedSorted, String actualString) {
    List<? extends RegionPruneInfo> actual = GSON.fromJson(actualString, PRUNE_INFO_LIST_TYPE);
    List<RegionPruneInfo> actualSorted = new ArrayList<>(actual);
    Collections.sort(actualSorted, pruneUpperBoundAndStringComparator());

    Assert.assertEquals(GSON.toJson(expectedSorted), GSON.toJson(actualSorted));
  }

  @SuppressWarnings("SameParameterValue")
  private <T> SortedSet<T> subset(SortedSet<T> set, int from, int to) {
    SortedSet<T> subset = new TreeSet<>(set.comparator());
    int i = from;
    for (T e : set) {
      if (i++ >= to) {
        break;
      }
      subset.add(e);
    }
    return subset;
  }
}
