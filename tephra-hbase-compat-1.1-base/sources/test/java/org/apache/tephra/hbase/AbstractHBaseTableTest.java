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

package org.apache.tephra.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;

/**
 * Base class for tests that need a HBase cluster
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractHBaseTableTest {
  protected static HBaseTestingUtility testUtil;
  protected static HBaseAdmin hBaseAdmin;
  protected static Configuration conf;

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    testUtil = conf == null ? new HBaseTestingUtility() : new HBaseTestingUtility(conf);
    conf = testUtil.getConfiguration();

    // Tune down the connection thread pool size
    conf.setInt("hbase.hconnection.threads.core", 5);
    conf.setInt("hbase.hconnection.threads.max", 10);
    // Tunn down handler threads in regionserver
    conf.setInt("hbase.regionserver.handler.count", 10);

    // Set to random port
    conf.setInt("hbase.master.port", 0);
    conf.setInt("hbase.master.info.port", 0);
    conf.setInt("hbase.regionserver.port", 0);
    conf.setInt("hbase.regionserver.info.port", 0);

    testUtil.startMiniCluster();
    hBaseAdmin = testUtil.getHBaseAdmin();
  }

  @AfterClass
  public static void shutdownMiniCluster() throws Exception {
    try {
      if (hBaseAdmin != null) {
        hBaseAdmin.close();
      }
    } finally {
      testUtil.shutdownMiniCluster();
    }
  }

  protected static HTable createTable(byte[] tableName, byte[][] columnFamilies) throws Exception {
    return createTable(tableName, columnFamilies, false,
                       Collections.singletonList(TransactionProcessor.class.getName()));
  }

  protected static HTable createTable(byte[] tableName, byte[][] columnFamilies, boolean existingData,
                                      List<String> coprocessors) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    for (byte[] family : columnFamilies) {
      HColumnDescriptor columnDesc = new HColumnDescriptor(family);
      columnDesc.setMaxVersions(Integer.MAX_VALUE);
      columnDesc.setValue(TxConstants.PROPERTY_TTL, String.valueOf(100000)); // in millis
      desc.addFamily(columnDesc);
    }
    if (existingData) {
      desc.setValue(TxConstants.READ_NON_TX_DATA, "true");
    }
    // Divide individually to prevent any overflow
    int priority = Coprocessor.PRIORITY_USER;
    // order in list is the same order that coprocessors will be invoked
    for (String coprocessor : coprocessors) {
      desc.addCoprocessor(coprocessor, null, ++priority, null);
    }
    hBaseAdmin.createTable(desc);
    testUtil.waitTableAvailable(tableName, 5000);
    return new HTable(testUtil.getConfiguration(), tableName);
  }
}
