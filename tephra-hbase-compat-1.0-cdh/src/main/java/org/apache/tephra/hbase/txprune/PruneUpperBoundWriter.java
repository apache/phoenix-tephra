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

package org.apache.tephra.hbase.txprune;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.tephra.hbase.coprocessor.TransactionProcessor;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread that will write the the prune upper bound
 */
public class PruneUpperBoundWriter {
  private static final Log LOG = LogFactory.getLog(TransactionProcessor.class);

  private final TableName pruneStateTable;
  private final DataJanitorState dataJanitorState;
  private final byte[] regionName;
  private final String regionNameAsString;
  private final long pruneFlushInterval;
  private final AtomicLong pruneUpperBound;
  private final AtomicBoolean shouldFlush;

  private Thread flushThread;
  private long lastChecked;

  public PruneUpperBoundWriter(DataJanitorState dataJanitorState, TableName pruneStateTable, String regionNameAsString,
                               byte[] regionName, long pruneFlushInterval) {
    this.pruneStateTable = pruneStateTable;
    this.dataJanitorState = dataJanitorState;
    this.regionName = regionName;
    this.regionNameAsString = regionNameAsString;
    this.pruneFlushInterval = pruneFlushInterval;
    this.pruneUpperBound = new AtomicLong();
    this.shouldFlush = new AtomicBoolean(false);
    startFlushThread();
  }

  public boolean isAlive() {
    return flushThread.isAlive();
  }

  public void persistPruneEntry(long pruneUpperBound) {
    this.pruneUpperBound.set(pruneUpperBound);
    this.shouldFlush.set(true);
  }

  public void stop() {
    if (flushThread != null) {
      flushThread.interrupt();
    }
  }

  private void startFlushThread() {
    flushThread = new Thread("tephra-prune-upper-bound-writer") {
      @Override
      public void run() {
        while (!isInterrupted()) {
          long now = System.currentTimeMillis();
          if (now > (lastChecked + pruneFlushInterval)) {
            if (shouldFlush.compareAndSet(true, false)) {
              // should flush data
              try {
                dataJanitorState.savePruneUpperBoundForRegion(regionName, pruneUpperBound.get());
              } catch (IOException ex) {
                LOG.warn("Cannot record prune upper bound for region " + regionNameAsString + " in the table " +
                           pruneStateTable.getNameWithNamespaceInclAsString() + " after compacting region.", ex);
                // Retry again
                shouldFlush.set(true);
              }
            }
            lastChecked = now;
          }

          try {
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException ex) {
            interrupt();
            break;
          }
        }

        LOG.info("PruneUpperBound Writer thread terminated.");
      }
    };

    flushThread.setDaemon(true);
    flushThread.start();
  }
}
