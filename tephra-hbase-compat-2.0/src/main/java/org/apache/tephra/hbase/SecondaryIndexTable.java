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
package org.apache.tephra.hbase;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.distributed.TransactionServiceClient;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Transactional SecondaryIndexTable.
 */
public class SecondaryIndexTable implements Closeable {
  private byte[] secondaryIndex;
  private TransactionAwareHTable transactionAwareHTable;
  private TransactionAwareHTable secondaryIndexTable;
  private TransactionContext transactionContext;
  private final TableName secondaryIndexTableName;
  private Connection connection;
  private static final byte[] secondaryIndexFamily = Bytes.toBytes("secondaryIndexFamily");
  private static final byte[] secondaryIndexQualifier = Bytes.toBytes('r');
  private static final byte[] DELIMITER  = new byte[] {0};

  public SecondaryIndexTable(TransactionServiceClient transactionServiceClient, Table table,
                             byte[] secondaryIndex) throws IOException {
    secondaryIndexTableName = TableName.valueOf(table.getName().getNameAsString() + ".idx");
    this.connection = ConnectionFactory.createConnection(table.getConfiguration());
    Table secondaryIndexHTable = null;
    try (Admin hBaseAdmin = this.connection.getAdmin()) {
      if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
        hBaseAdmin.createTable(TableDescriptorBuilder.newBuilder(secondaryIndexTableName).build());
      }
      secondaryIndexHTable = this.connection.getTable(secondaryIndexTableName);
    } catch (Exception e) {
      Closeables.closeQuietly(connection);
      Throwables.propagate(e);
    }

    this.secondaryIndex = secondaryIndex;
    this.transactionAwareHTable = new TransactionAwareHTable(table);
    this.secondaryIndexTable = new TransactionAwareHTable(secondaryIndexHTable);
    this.transactionContext = new TransactionContext(transactionServiceClient, transactionAwareHTable,
                                                     secondaryIndexTable);
  }

  public Result get(Get get) throws IOException {
    return get(Collections.singletonList(get))[0];
  }

  public Result[] get(List<Get> gets) throws IOException {
    try {
      transactionContext.start();
      Result[] result = transactionAwareHTable.get(gets);
      transactionContext.finish();
      return result;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction", e1);
      }
    }
    return null;
  }

  public Result[] getByIndex(byte[] value) throws IOException {
    try {
      transactionContext.start();
      Scan scan = new Scan(value, Bytes.add(value, new byte[0]));
      scan.addColumn(secondaryIndexFamily, secondaryIndexQualifier);
      ResultScanner indexScanner = secondaryIndexTable.getScanner(scan);

      ArrayList<Get> gets = new ArrayList<>();
      for (Result result : indexScanner) {
        for (Cell cell : result.listCells()) {
          gets.add(new Get(CellUtil.cloneValue(cell)));
        }
      }
      Result[] results = transactionAwareHTable.get(gets);
      transactionContext.finish();
      return results;
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction", e1);
      }
    }
    return null;
  }

  public void put(Put put) throws IOException {
    put(Collections.singletonList(put));
  }


  public void put(List<Put> puts) throws IOException {
    try {
      transactionContext.start();
      ArrayList<Put> secondaryIndexPuts = new ArrayList<>();
      for (Put put : puts) {
        List<Put> indexPuts = new ArrayList<>();
        Set<Map.Entry<byte[], List<Cell>>> familyMap = put.getFamilyCellMap().entrySet();
        for (Map.Entry<byte [], List<Cell>> family : familyMap) {
          for (Cell value : family.getValue()) {
            if (Bytes.equals(value.getQualifierArray(), value.getQualifierOffset(), value.getQualifierLength(),
                             secondaryIndex, 0, secondaryIndex.length)) {
              byte[] secondaryRow = Bytes.add(CellUtil.cloneQualifier(value), DELIMITER,
                                                    Bytes.add(CellUtil.cloneValue(value), DELIMITER,
                                                          CellUtil.cloneRow(value)));
              Put indexPut = new Put(secondaryRow);
              indexPut.addColumn(secondaryIndexFamily, secondaryIndexQualifier, put.getRow());
              indexPuts.add(indexPut);
            }
          }
        }
        secondaryIndexPuts.addAll(indexPuts);
      }
      transactionAwareHTable.put(puts);
      secondaryIndexTable.put(secondaryIndexPuts);
      transactionContext.finish();
    } catch (Exception e) {
      try {
        transactionContext.abort();
      } catch (TransactionFailureException e1) {
        throw new IOException("Could not rollback transaction", e1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    Closeables.closeQuietly(connection);
    try {
      transactionAwareHTable.close();
    } catch (IOException e) {
      try {
        secondaryIndexTable.close();
      } catch (IOException ex) {
        e.addSuppressed(ex);
      }
      throw e;
    }
    secondaryIndexTable.close();
  }
}
