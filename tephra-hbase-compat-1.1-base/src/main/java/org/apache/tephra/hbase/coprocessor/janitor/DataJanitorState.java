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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;

/**
 * Persist data janitor state into an HBase table.
 * This is used by both {@link TransactionProcessor} and by the {@link HBaseTransactionPruningPlugin}
 * to persist and read the compaction state.
 */
@SuppressWarnings("WeakerAccess")
public class DataJanitorState {
  public static final byte[] FAMILY = {'f'};

  private static final byte[] PRUNE_UPPER_BOUND_COL = {'p'};
  private static final byte[] REGION_TIME_COL = {'r'};
  private static final byte[] INACTIVE_TRANSACTION_BOUND_TIME_COL = {'i'};

  private static final byte[] REGION_KEY_PREFIX = {0x1};
  private static final byte[] REGION_KEY_PREFIX_STOP = {0x2};

  private static final byte[] REGION_TIME_KEY_PREFIX = {0x2};
  private static final byte[] REGION_TIME_KEY_PREFIX_STOP = {0x3};

  private static final byte[] INACTIVE_TRANSACTION_BOUND_TIME_KEY_PREFIX = {0x3};
  private static final byte[] INACTIVE_TRANSACTION_BOUND_TIME_KEY_PREFIX_STOP = {0x4};

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final TableSupplier stateTableSupplier;


  public DataJanitorState(TableSupplier stateTableSupplier) {
    this.stateTableSupplier = stateTableSupplier;
  }

  // ----------------------------------------------------------------
  // ------- Methods for prune upper bound for a given region -------
  // ----------------------------------------------------------------
  // The data is stored in the following format -
  // Key: 0x1<region-id>
  // Col 'u': <prune upper bound>
  // ----------------------------------------------------------------

  /**
   * Persist the latest prune upper bound for a given region. This is called by {@link TransactionProcessor}
   * after major compaction.
   *
   * @param regionId region id
   * @param pruneUpperBound the latest prune upper bound for the region
   * @throws IOException when not able to persist the data to HBase
   */
  public void savePruneUpperBoundForRegion(byte[] regionId, long pruneUpperBound) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Put put = new Put(makeRegionKey(regionId));
      put.addColumn(FAMILY, PRUNE_UPPER_BOUND_COL, Bytes.toBytes(pruneUpperBound));
      stateTable.put(put);
    }
  }

  /**
   * Get latest prune upper bound for a given region. This indicates the largest invalid transaction that no
   * longer has writes in this region.
   *
   * @param regionId region id
   * @return latest prune upper bound for the region
   * @throws IOException when not able to read the data from HBase
   */
  public long getPruneUpperBoundForRegion(byte[] regionId) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Get get = new Get(makeRegionKey(regionId));
      get.addColumn(FAMILY, PRUNE_UPPER_BOUND_COL);
      byte[] result = stateTable.get(get).getValue(FAMILY, PRUNE_UPPER_BOUND_COL);
      return result == null ? -1 : Bytes.toLong(result);
    }
  }

  /**
   * Get latest prune upper bounds for given regions. This is a batch operation of method
   * {@link #getPruneUpperBoundForRegion(byte[])}
   *
   * @param regions a set of regions
   * @return a map containing region id and its latest prune upper bound value
   * @throws IOException when not able to read the data from HBase
   */
  public Map<byte[], Long> getPruneUpperBoundForRegions(SortedSet<byte[]> regions) throws IOException {
    Map<byte[], Long> resultMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    try (Table stateTable = stateTableSupplier.get()) {
      byte[] startRow = makeRegionKey(EMPTY_BYTE_ARRAY);
      Scan scan = new Scan(startRow, REGION_KEY_PREFIX_STOP);
      scan.addColumn(FAMILY, PRUNE_UPPER_BOUND_COL);

      try (ResultScanner scanner = stateTable.getScanner(scan)) {
        Result next;
        while ((next = scanner.next()) != null) {
          byte[] region = getRegionFromKey(next.getRow());
          if (regions.contains(region)) {
            byte[] timeBytes = next.getValue(FAMILY, PRUNE_UPPER_BOUND_COL);
            if (timeBytes != null) {
              long pruneUpperBoundRegion = Bytes.toLong(timeBytes);
              resultMap.put(region, pruneUpperBoundRegion);
            }
          }
        }
      }
      return resultMap;
    }
  }

  /**
   * Delete prune upper bounds for the regions that are not in the given exclude set, and the
   * prune upper bound is less than the given value.
   * After the invalid list is pruned up to deletionPruneUpperBound, we do not need entries for regions that have
   * prune upper bound less than deletionPruneUpperBound. We however limit the deletion to only regions that are
   * no longer in existence (due to deletion, etc.), to avoid update/delete race conditions.
   *
   * @param deletionPruneUpperBound prune upper bound below which regions will be deleted
   * @param excludeRegions set of regions that should not be deleted
   * @throws IOException when not able to delete data in HBase
   */
  public void deletePruneUpperBounds(long deletionPruneUpperBound, SortedSet<byte[]> excludeRegions)
    throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      byte[] startRow = makeRegionKey(EMPTY_BYTE_ARRAY);
      Scan scan = new Scan(startRow, REGION_KEY_PREFIX_STOP);
      scan.addColumn(FAMILY, PRUNE_UPPER_BOUND_COL);

      try (ResultScanner scanner = stateTable.getScanner(scan)) {
        Result next;
        while ((next = scanner.next()) != null) {
          byte[] region = getRegionFromKey(next.getRow());
          if (!excludeRegions.contains(region)) {
            byte[] timeBytes = next.getValue(FAMILY, PRUNE_UPPER_BOUND_COL);
            if (timeBytes != null) {
              long pruneUpperBoundRegion = Bytes.toLong(timeBytes);
              if (pruneUpperBoundRegion < deletionPruneUpperBound) {
                stateTable.delete(new Delete(next.getRow()));
              }
            }
          }
        }
      }
    }
  }

  // ---------------------------------------------------
  // ------- Methods for regions at a given time -------
  // ---------------------------------------------------
  // Key: 0x2<time><region-id>
  // Col 't': <empty byte array>
  // ---------------------------------------------------

  /**
   * Persist the regions for the given time. {@link HBaseTransactionPruningPlugin} saves the set of
   * transactional regions existing in the HBase instance periodically.
   *
   * @param time timestamp in milliseconds
   * @param regions set of regions at the time
   * @throws IOException when not able to persist the data to HBase
   */
  public void saveRegionsForTime(long time, Set<byte[]> regions) throws IOException {
    byte[] timeBytes = Bytes.toBytes(getInvertedTime(time));
    try (Table stateTable = stateTableSupplier.get()) {
      for (byte[] region : regions) {
        Put put = new Put(makeTimeRegionKey(timeBytes, region));
        put.addColumn(FAMILY, REGION_TIME_COL, EMPTY_BYTE_ARRAY);
        stateTable.put(put);
      }
    }
  }

  /**
   * Return the set of regions saved for the time at or before the given time. This method finds the greatest time
   * that is less than or equal to the given time, and then returns all regions with that exact time, but none that are
   * older than that.
   *
   * @param time timestamp in milliseconds
   * @return set of regions and time at which they were recorded, or null if no regions found
   * @throws IOException when not able to read the data from HBase
   */
  @Nullable
  public TimeRegions getRegionsOnOrBeforeTime(long time) throws IOException {
    byte[] timeBytes = Bytes.toBytes(getInvertedTime(time));
    try (Table stateTable = stateTableSupplier.get()) {
      Scan scan = new Scan(makeTimeRegionKey(timeBytes, EMPTY_BYTE_ARRAY), REGION_TIME_KEY_PREFIX_STOP);
      scan.addColumn(FAMILY, REGION_TIME_COL);

      SortedSet<byte[]> regions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      long currentRegionTime = -1;
      try (ResultScanner scanner = stateTable.getScanner(scan)) {
        Result next;
        while ((next = scanner.next()) != null) {
          Map.Entry<Long, byte[]> timeRegion = getTimeRegion(next.getRow());
          // Stop if reached next time value
          if (currentRegionTime == -1) {
            currentRegionTime = timeRegion.getKey();
          } else if (timeRegion.getKey() < currentRegionTime) {
            break;
          } else if (timeRegion.getKey() > currentRegionTime) {
            throw new IllegalStateException(
              String.format("Got out of order time %d when expecting time less than or equal to %d",
                            timeRegion.getKey(), currentRegionTime));
          }
          regions.add(timeRegion.getValue());
        }
      }
      return regions.isEmpty() ? null : new TimeRegions(currentRegionTime, regions);
    }
  }

  /**
   * Delete all the regions that were recorded for all times equal or less than the given time.
   *
   * @param time timestamp in milliseconds
   * @throws IOException when not able to delete data in HBase
   */
  public void deleteAllRegionsOnOrBeforeTime(long time) throws IOException {
    byte[] timeBytes = Bytes.toBytes(getInvertedTime(time));
    try (Table stateTable = stateTableSupplier.get()) {
      Scan scan = new Scan(makeTimeRegionKey(timeBytes, EMPTY_BYTE_ARRAY), REGION_TIME_KEY_PREFIX_STOP);
      scan.addColumn(FAMILY, REGION_TIME_COL);

      try (ResultScanner scanner = stateTable.getScanner(scan)) {
        Result next;
        while ((next = scanner.next()) != null) {
          stateTable.delete(new Delete(next.getRow()));
        }
      }
    }
  }

  // ---------------------------------------------------------------------
  // ------- Methods for inactive transaction bound for given time -------
  // ---------------------------------------------------------------------
  // Key: 0x3<inverted time>
  // Col 'p': <inactive transaction bound>
  // ---------------------------------------------------------------------

  /**
   * Persist inactive transaction bound for a given time. This is the smallest not in-progress transaction that
   * will not have writes in any HBase regions that are created after the given time.
   *
   * @param time time in milliseconds
   * @param inactiveTransactionBound inactive transaction bound for the given time
   * @throws IOException when not able to persist the data to HBase
   */
  public void saveInactiveTransactionBoundForTime(long time, long inactiveTransactionBound) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Put put = new Put(makeInactiveTransactionBoundTimeKey(Bytes.toBytes(getInvertedTime(time))));
      put.addColumn(FAMILY, INACTIVE_TRANSACTION_BOUND_TIME_COL, Bytes.toBytes(inactiveTransactionBound));
      stateTable.put(put);
    }
  }

  /**
   * Return inactive transaction bound for the given time.
   *
   * @param time time in milliseconds
   * @return inactive transaction bound for the given time
   * @throws IOException when not able to read the data from HBase
   */
  public long getInactiveTransactionBoundForTime(long time) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Get get = new Get(makeInactiveTransactionBoundTimeKey(Bytes.toBytes(getInvertedTime(time))));
      get.addColumn(FAMILY, INACTIVE_TRANSACTION_BOUND_TIME_COL);
      byte[] result = stateTable.get(get).getValue(FAMILY, INACTIVE_TRANSACTION_BOUND_TIME_COL);
      return result == null ? -1 : Bytes.toLong(result);
    }
  }

  /**
   * Delete all inactive transaction bounds recorded for a time less than the given time
   *
   * @param time time in milliseconds
   * @throws IOException when not able to delete data in HBase
   */
  public void deleteInactiveTransactionBoundsOnOrBeforeTime(long time) throws IOException {
    try (Table stateTable = stateTableSupplier.get()) {
      Scan scan = new Scan(makeInactiveTransactionBoundTimeKey(Bytes.toBytes(getInvertedTime(time))),
                           INACTIVE_TRANSACTION_BOUND_TIME_KEY_PREFIX_STOP);
      scan.addColumn(FAMILY, INACTIVE_TRANSACTION_BOUND_TIME_COL);

      try (ResultScanner scanner = stateTable.getScanner(scan)) {
        Result next;
        while ((next = scanner.next()) != null) {
          stateTable.delete(new Delete(next.getRow()));
        }
      }
    }
  }

  private byte[] makeRegionKey(byte[] regionId) {
    return Bytes.add(REGION_KEY_PREFIX, regionId);
  }

  private byte[] getRegionFromKey(byte[] regionKey) {
    int prefixLen = REGION_KEY_PREFIX.length;
    return Bytes.copy(regionKey, prefixLen, regionKey.length - prefixLen);
  }

  private byte[] makeTimeRegionKey(byte[] time, byte[] regionId) {
    return Bytes.add(REGION_TIME_KEY_PREFIX, time, regionId);
  }

  private byte[] makeInactiveTransactionBoundTimeKey(byte[] time) {
    return Bytes.add(INACTIVE_TRANSACTION_BOUND_TIME_KEY_PREFIX, time);
  }

  private Map.Entry<Long, byte[]> getTimeRegion(byte[] key) {
    int offset = REGION_TIME_KEY_PREFIX.length;
    long time = getInvertedTime(Bytes.toLong(key, offset));
    offset += Bytes.SIZEOF_LONG;
    byte[] regionName = Bytes.copy(key, offset, key.length - offset);
    return Maps.immutableEntry(time, regionName);
  }

  private long getInvertedTime(long time) {
    return Long.MAX_VALUE - time;
  }

  /**
   * Supplies table for persisting state
   */
  public interface TableSupplier {
    Table get() throws IOException;
  }
}
