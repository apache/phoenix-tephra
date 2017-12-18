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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests to verify the behavior of {@link PruneUpperBoundWriterSupplier}.
 */
public class PruneUpperBoundWriterSupplierTest {
  private static final Logger LOG = LoggerFactory.getLogger(PruneUpperBoundWriterSupplierTest.class);
  private static final int NUM_OPS = 10000;
  private static final int NUM_THREADS = 50;

  @Test
  public void testSupplier() throws Exception {
    final PruneUpperBoundWriterSupplier supplier = new PruneUpperBoundWriterSupplier(null, null, 10L);
    // Get one instance now, for later comparisons
    final PruneUpperBoundWriter writer = supplier.get();
    final AtomicInteger numOps = new AtomicInteger(NUM_OPS);
    final Random random = new Random(System.currentTimeMillis());

    // Start threads that will 'get' PruneUpperBoundWriters
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future> futureList = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      futureList.add(executor.submit(new Runnable() {

        @Override
        public void run() {
          // Perform NUM_OPS 'gets' of PruneUpperBoundWriter
          while (numOps.decrementAndGet() > 0) {
            PruneUpperBoundWriter newWriter = supplier.get();
            Assert.assertTrue(newWriter == writer);
            int waitTime = random.nextInt(10);
            try {
              TimeUnit.MICROSECONDS.sleep(waitTime);
            } catch (InterruptedException e) {
              LOG.warn("Received an exception.", e);
            }
          }
        }
      }));
    }

    for (Future future : futureList) {
      future.get(5, TimeUnit.SECONDS);
    }
    executor.shutdown();
    executor.awaitTermination(2, TimeUnit.SECONDS);

    futureList.clear();
    numOps.set(NUM_OPS);
    // Start thread that release PruneUpperBoundWriters
    executor = Executors.newFixedThreadPool(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      futureList.add(executor.submit(new Runnable() {

        @Override
        public void run() {
          // We need to release all NUM_OPS 'gets' that were executed to trigger shutdown of the single instance of
          // PruneUpperBoundWriter
          while (numOps.decrementAndGet() > 0) {
            supplier.release();
            try {
              TimeUnit.MICROSECONDS.sleep(random.nextInt(10));
            } catch (InterruptedException e) {
              LOG.warn("Received an exception.", e);
            }
          }
        }
      }));
    }

    for (Future future : futureList) {
      future.get(1, TimeUnit.SECONDS);
    }

    executor.shutdown();
    executor.awaitTermination(2, TimeUnit.SECONDS);

    // Verify that the PruneUpperBoundWriter is still running and the pruneThread is still alive.
    Assert.assertTrue(writer.isRunning());
    Assert.assertTrue(writer.isAlive());

    // Since we got one instance in the beginning, we need to release it
    supplier.release();

    // Verify that the PruneUpperBoundWriter is shutdown and the pruneThread is not alive anymore.
    Assert.assertFalse(writer.isRunning());
    Assert.assertFalse(writer.isAlive());
  }
}
