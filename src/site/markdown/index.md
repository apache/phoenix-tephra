<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License. You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

<head>
  <title>Home</title>
</head>

## What is Apache Tephra <sup>(TM)</sup>
Apache Tephra provides globally consistent transactions on top of distributed data stores
such as [Apache HBase](https://hbase.apache.org). While HBase provides strong consistency with row- or
region-level ACID operations, it sacrifices cross-region and cross-table consistency in favor of
scalability. This trade-off requires application developers to handle the complexity of ensuring
consistency when their modifications span region boundaries. By providing support for global
transactions that span regions, tables, or multiple RPCs, Tephra simplifies application development
on top of HBase, without a significant impact on performance or scalability for many workloads.

Tephra is used by the [Apache Phoenix](https://phoenix.apache.org/transactions.html) as well
to add cross-row and cross-table transaction support with full ACID semantics.

## How It Works
Tephra leverages HBase's native data versioning to provide multi-versioned concurrency
control (MVCC) for transactional reads and writes. With MVCC capability, each transaction
sees its own consistent "snapshot" of data, providing
[snapshot isolation](http://en.wikipedia.org/wiki/Snapshot_isolation) of concurrent transactions.

Tephra consists of three main components:

- __Transaction Server__ - maintains global view of transaction state, assigns new transaction IDs
  and performs conflict detection;
- __Transaction Client__ - coordinates start, commit, and rollback of transactions; and
- __TransactionProcessor Coprocessor__ - applies filtering to the data read (based on a
  given transaction's state) and cleans up any data from old (no longer visible) transactions.

### Transaction Server
A central transaction manager generates a globally unique, time-based transaction ID for each
transaction that is started, and maintains the state of all in-progress and recently committed
transactions for conflict detection. While multiple transaction server instances can be run
concurrently for automatic failover, only one server instance is actively serving requests at a
time. This is coordinated by performing leader election amongst the running instances through
[Apache ZooKeeper](https://zookeeper.apache.org). The active transaction server instance will
also register itself using a service discovery interface in ZooKeeper, allowing clients to
discover the currently active server instance without additional configuration.

### Transaction Client
A client makes a call to the active transaction server in order to start a new transaction. This
returns a new transaction instance to the client, with a unique transaction ID (used to identify
writes for the transaction), as well as a list of transaction IDs to exclude for reads (from
in-progress or invalidated transactions). When performing writes, the client overrides the
timestamp for all modified HBase cells with the transaction ID. When reading data from HBase, the
client skips cells associated with any of the excluded transaction IDs. The read exclusions are
applied through a server-side filter injected by the `TransactionProcessor` coprocessor.

### TransactionProcessor Coprocessor
The `TransactionProcessor` coprocessor is loaded on all HBase tables where transactional reads
and writes are performed. When clients read data, it coordinates the server-side filtering
performed based on the client transaction's snapshot. Data cells from any transactions that are
currently in-progress or those that have failed and could not be rolled back ("invalid"
transactions) will be skipped on these reads. In addition, the `TransactionProcessor` cleans
up any data versions that are no longer visible to any running transactions, either because the
transaction that the cell is associated with failed or a write from a newer transaction was
successfully committed to the same column.

More details on how Tephra transactions work and the interactions between these components can be
found in our [Presentations](Presentations.html).

## Is It Building?
Status of CI build at Travis CI: [![Build Status](https://travis-ci.org/apache/incubator-tephra.svg?branch=master)](https://travis-ci.org/apache/incubator-tephra)

## Requirements
### Java Runtime
The latest [JDK or JRE version 1.7.xx or 1.8.xx](http://www.java.com/en/download/manual.jsp)
for Linux, Windows, or Mac OS X must be installed in your environment; we recommend the Oracle JDK.

To check the Java version installed, run the command:

  $ java -version

Tephra is tested with the Oracle JDKs; it may work with other JDKs such as
[Open JDK](http://openjdk.java.net), but it has not been tested with them.

Once you have installed the JDK, you'll need to set the `JAVA_HOME` environment variable.

### Hadoop/HBase Environment
Tephra requires a working HBase and HDFS environment in order to operate. Tephra supports these
component versions:

| Component     | Source        | Supported Versions                                              |
|---------------|---------------|-----------------------------------------------------------------|
| __HDFS__      | Apache Hadoop | 2.0.2-alpha through 2.7.x                                       |
|               | CDH or HDP    | (CDH) 5.0.0 through 5.8.0 or (HDP) 2.0, 2.1, 2.2, 2.3 or 2.4    |
|               | MapR          | 4.1 through 5.1 (with MapR-FS)                                  |
| __HBase__     | Apache        | 0.96.x, 0.98.x, 1.0.x, 1.1.x and 1.2.x (except 1.1.5 and 1.2.2) |
|               | CDH or HDP    | (CDH) 5.0.0 through 5.8.0 or (HDP) 2.0, 2.1, 2.2, 2.3 or 2.4    |
|               | MapR          | 4.1 through 5.1 (with Apache HBase)                             |
| __ZooKeeper__ | Apache        | Version 3.4.3 through 3.4.5                                     |
|               | CDH or HDP    | (CDH) 5.0.0 through 5.8.0 or (HDP) 2.0, 2.1, 2.2, 2.3 or 2.4    |
|               | MapR          | 4.1 through 5.1                                                 |

__Note:__ Components versions shown in this table are those that we have tested and are
confident of their suitability and compatibility. Later versions of components may work,
but have not necessarily been either tested or confirmed compatible.

Ready to try out Apache Tephra? Checkout the [Getting Started Guide](GettingStarted.html)

## Disclaimer
Apache Tephra is an effort undergoing incubation at The Apache Software Foundation (ASF),
sponsored by Incubator. Incubation is required of all newly accepted projects until a
further review indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects. While incubation
status is not necessarily a reflection of the completeness or stability of the code, it does
indicate that the project has yet to be fully endorsed by the ASF.
