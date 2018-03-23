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

import org.apache.tephra.TxConstants.ConflictDetection;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AbstractTransactionAwareTableTest {
  public static final byte[] TABLE_NAME = "a".getBytes();
  public static final String ROW1 = "r1";
  public static final String ROW2 = "r2";
  public static final String FAM1 = "f1";

  private static class ConcreteTransactionAwareTable extends AbstractTransactionAwareTable {
    private final byte[] tableKey;

    public ConcreteTransactionAwareTable(ConflictDetection conflictLevel,
        boolean allowNonTransactional, byte[] tableKey) {
      super(conflictLevel, allowNonTransactional);
      this.tableKey = tableKey;
    }

    @Override
    protected boolean doCommit() throws IOException {
      return false;
    }

    @Override
    protected byte[] getTableKey() {
      return tableKey;
    }

    @Override
    protected boolean doRollback() throws Exception {
      return false;
    }

    public Set<ActionChange> getChangeSet() {
      return changeSets.isEmpty() ? 
          Collections.<ActionChange>emptySet() : 
            changeSets.values().iterator().next();
    }
  }

  @Test
  public void testActionChangeEquality() {
    long wp = System.currentTimeMillis();
    long rp = wp - 100;
    ConcreteTransactionAwareTable table1 = 
        new ConcreteTransactionAwareTable(ConflictDetection.ROW, false, TABLE_NAME);
    ConcreteTransactionAwareTable table2 = 
        new ConcreteTransactionAwareTable(ConflictDetection.ROW, false, TABLE_NAME);
    Transaction tx = new Transaction(rp, wp, new long[] {}, new long[] {}, wp);
    table1.startTx(tx);
    table2.startTx(tx);
    table1.addToChangeSet(ROW1.getBytes(), FAM1.getBytes(), null);
    table1.addToChangeSet(ROW1.getBytes(), FAM1.getBytes(), null);
    table2.addToChangeSet(ROW1.getBytes(), FAM1.getBytes(), null);
    assertEquals(table1.getChangeSet(), table2.getChangeSet());
    table1.addToChangeSet(ROW2.getBytes(), FAM1.getBytes(), null);
    table2.addToChangeSet(ROW2.getBytes(), FAM1.getBytes(), null);
    table2.addToChangeSet(ROW2.getBytes(), FAM1.getBytes(), null);
    assertEquals(table1.getChangeSet(), table2.getChangeSet());
  }
}
