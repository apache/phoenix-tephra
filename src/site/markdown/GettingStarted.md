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
  <title>Getting Started</title>
</head>

## Getting Started
You can get started with Tephra by building directly from the latest source code:

```sh
  git clone https://git-wip-us.apache.org/repos/asf/incubator-tephra.git
  cd incubator-tephra
  mvn clean package
```

After the build completes, you will have a full binary distribution of Tephra under the
`tephra-distribution/target/` directory. Take the `tephra-<version>.tar.gz` file and install
it on your systems.

For any client applications, add the following dependencies to any Apache Maven POM files (or your
build system's equivalent configuration), in order to make use of Tephra classes:

```xml
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-api</artifactId>
    <version>${tephra.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-core</artifactId>
    <version>${tephra.version}</version>
  </dependency>
```

Since the HBase APIs have changed between versions, you will need to select the
appropriate HBase compatibility library.

For HBase 0.96.x:

```xml
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-hbase-compat-0.96</artifactId>
    <version>${tephra.version}</version>
  </dependency>
```

For HBase 0.98.x:

```xml
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-hbase-compat-0.98</artifactId>
    <version>${tephra.version}</version>
  </dependency>
```

For HBase 1.0.x:

```xml
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-hbase-compat-1.0</artifactId>
    <version>${tephra.version}</version>
  </dependency>
```

If you are running the CDH 5.4, 5.5, or 5.6 version of HBase 1.0.x (this version contains API incompatibilities
with Apache HBase 1.0.x):

```xml
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-hbase-compat-1.0-cdh</artifactId>
    <version>${tephra.version}</version>
  </dependency>
```

For HBase 1.1.x or HBase 1.2.x:

```xml
  <dependency>
    <groupId>org.apache.tephra</groupId>
    <artifactId>tephra-hbase-compat-1.1</artifactId>
    <version>${tephra.version}</version>
  </dependency>
```

## Deployment and Configuration
Tephra makes use of a central transaction server to assign unique transaction IDs for data
modifications and to perform conflict detection. Only a single transaction server can actively
handle client requests at a time, however, additional transaction server instances can be run
simultaneously, providing automatic failover if the active server becomes unreachable.

### Transaction Server Configuration
The Tephra transaction server can be deployed on the same cluster nodes running the HBase HMaster
process. The transaction server requires that the HBase libraries be available on the server's
Java `CLASSPATH`.

The transaction server supports the following configuration properties. All configuration
properties can be added to the `hbase-site.xml` file on the server's `CLASSPATH`:


| Name                        | Default    | Description                                                     |
|-----------------------------|------------|-----------------------------------------------------------------|
| `data.tx.bind.port`         | 15165      | Port to bind to                                                 |
| `data.tx.bind.address`      | 0.0.0.0    | Server address to listen on                                     |
| `data.tx.server.io.threads` | 2          | Number of threads for socket IO                                 |
| `data.tx.server.threads`    | 20         | Number of handler threads                                       |
| `data.tx.timeout`           | 30         | Timeout for a transaction to complete (seconds)                 |
| `data.tx.long.timeout`      | 86400      | Timeout for a long running transaction to complete (seconds)    |
| `data.tx.cleanup.interval`  | 10         | Frequency to check for timed out transactions (seconds)         |
| `data.tx.snapshot.dir`      |            | HDFS directory used to store snapshots of tx state              |
| `data.tx.snapshot.interval` | 300        | Frequency to write new snapshots                                |
| `data.tx.snapshot.retain`   | 10         | Number of old transaction snapshots to retain                   |
| `data.tx.metrics.period`    | 60         | Frequency for metrics reporting (seconds)                       |

To run the Transaction server, execute the following command in your Tephra installation:

```sh
  ./bin/tephra start
```

Any environment-specific customizations can be made by editing the `bin/tephra-env.sh` script.


### Client Configuration
Since Tephra clients will be communicating with HBase, the HBase client libraries and the HBase cluster
configuration must be available on the client's Java `CLASSPATH`.

Client API usage is described in the Client APIs section.

The transaction service client supports the following configuration properties. All configuration
properties can be added to the `hbase-site.xml` file on the client's `CLASSPATH`:

| Name                                   | Default   | Description                                   |
|----------------------------------------|-----------|-----------------------------------------------|
| `data.tx.client.timeout`               | 30000     | Client socket timeout (milliseconds)          |
| `data.tx.client.provider`              | pool      | Client provider strategy: <ul><li>"pool" uses a pool of clients</li><li>"thread-local" a client per thread</li></ul> Note that "thread-local" provider can have a resource leak if threads are recycled |
| `data.tx.client.count`                 | 50        | Max number of clients for "pool" provider     |
| `data.tx.client.obtain.timeout`        | 3000      | Timeout (milliseconds) to wait when obtaining clients from the "pool" provider |
| `data.tx.client.retry.strategy`        | backoff   | Client retry strategy: "backoff" for back off between attempts; "n-times" for fixed number of tries |
| `data.tx.client.retry.attempts`        | 2         | Number of times to retry ("n-times" strategy) |
| `data.tx.client.retry.backoff.initial` | 100       | Initial sleep time ("backoff" strategy)       |
| `data.tx.client.retry.backoff.factor`  | 4         | Multiplication factor for sleep time          |
| `data.tx.client.retry.backoff.limit`   | 30000     | Exit when sleep time reaches this limit       |


### HBase Coprocessor Configuration

In addition to the transaction server, Tephra requires an HBase coprocessor to be installed on all
tables where transactional reads and writes will be performed.

To configure the coprocessor on all HBase tables, add the following to `hbase-site.xml`:

```xml
  <property>
    <name>hbase.coprocessor.region.classes</name>
    <value>org.apache.tephra.hbase.coprocessor.TransactionProcessor</value>
  </property>
```

You may configure the `TransactionProcessor` to be loaded only on HBase tables that you will
be using for transaction reads and writes. However, you must ensure that the coprocessor is
available on all impacted tables in order for Tephra to function correctly.

### Using Existing HBase Tables Transactionally

Tephra overrides HBase cell timestamps with transaction IDs, and uses these transaction
IDs to filter out cells older than the TTL (Time-To-Live). Transaction IDs are at a higher
scale than cell timestamps. When a regular HBase table that has existing data is
converted to a transactional table, existing data may be filtered out during reads. To
allow reading of existing data from a transactional table, you will need to set the
property `data.tx.read.pre.existing` as `true` on the table's table descriptor.

Note that even without the property `data.tx.read.pre.existing` being set to `true`,
any existing data will not be removed during compactions. Existing data simply won't be
visible during reads.

### Metrics Reporting

Tephra ships with built-in support for reporting metrics via JMX and a log file, using the
[Dropwizard Metrics](http://metrics.dropwizard.io) library.

To enable JMX reporting for metrics, you will need to enable JMX in the Java runtime
arguments. Edit the `bin/tephra-env.sh` script and uncomment the following lines, making any
desired changes to configuration for port used, SSL, and JMX authentication:

```sh
  export JMX_OPTS="-Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=13001"
  export OPTS="$OPTS $JMX_OPTS"
```

To enable file-based reporting for metrics, edit the `conf/logback.xml` file and uncomment the
following section, replacing the `FILE-PATH` placeholder with a valid directory on the local
filesystem:

```xml
  <appender name="METRICS" class="ch.qos.logback.core.rolling.RollingFileAppender">
    <file>/FILE-PATH/metrics.log</file>
    <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
      <fileNamePattern>metrics.log.%d{yyyy-MM-dd}</fileNamePattern>
      <maxHistory>30</maxHistory>
    </rollingPolicy>
    <encoder>
      <pattern>%d{ISO8601} %msg%n</pattern>
    </encoder>
  </appender>
  <logger name="tephra-metrics" level="TRACE" additivity="false">
    <appender-ref ref="METRICS" />
  </logger>
```

The frequency of metrics reporting may be configured by setting the `data.tx.metrics.period`
configuration property to the report frequency in seconds.


## Client APIs

The `TransactionAwareHTable` class implements HBase's `HTableInterface`, thus providing the same APIs
that a standard HBase `HTable` instance provides. Only certain operations are supported
transactionally. These are:

| Methods Supported In Transactions                                                                 |
|---------------------------------------------------------------------------------------------------|
| `exists(Get get)`                                                                                 |
| `exists(List<Get> gets)`                                                                          |
| `get(Get get)`                                                                                    |
| `get(List<Get> gets)`                                                                             |
| `batch(List<? extends Row> actions, Object[] results)`                                            |
| `batch(List<? extends Row> actions)`                                                              |
| `batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)` [0.96] |
| `batchCallback(List<? extends Row> actions, Batch.Callback<R> callback)` [0.96]                   |
| `getScanner(byte[] family)`                                                                       |
| `getScanner(byte[] family, byte[] qualifier)`                                                     |
| `put(Put put)`                                                                                    |
| `put(List<Put> puts)`                                                                             |
| `delete(Delete delete)`                                                                           |
| `delete(List<Delete> deletes)`                                                                    |

Other operations are not supported transactionally and will throw an ``UnsupportedOperationException`` if invoked.
To allow use of these non-transactional operations, call ``setAllowNonTransactional(true)``. This
allows you to call the following methods non-transactionally:

| Methods Supported Outside of Transactions                                                               |
|---------------------------------------------------------------------------------------------------------|
| `getRowOrBefore(byte[] row, byte[], family)`                                                            |
| `checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)`                       |
| `checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete)`              |
| `mutateRow(RowMutations rm)`                                                                            |
| `append(Append append)`                                                                                 |
| `increment(Increment increment)`                                                                        |
| `incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)`                        |
| `incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)` |
| `incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL)`    |

Note that for `batch` operations, only certain supported operations (`get`, `put`, and `delete`)
are applied transactionally.

### Usage

To use a `TransactionalAwareHTable`, you need an instance of `TransactionContext`.
`TransactionContext` provides the basic contract for client use of transactions. At each point
in the transaction lifecycle, it provides the necessary interactions with the Tephra Transaction
Server in order to start, commit, and rollback transactions. Basic usage of
`TransactionContext` is handled using the following pattern:

```java
  TransactionContext context = new TransactionContext(client, transactionAwareHTable);
  try {
    context.start();
    transactionAwareHTable.put(new Put(Bytes.toBytes("row"));
    // ...
    context.finish();
  } catch (TransactionFailureException e) {
    context.abort();
  }
```

1. First, a new transaction is started using `TransactionContext.start()`.
1. Next, any data operations are performed within the context of the transaction.
1. After data operations are complete, `TransactionContext.finish()` is called to commit the
   transaction.
1. If an exception occurs, `TransactionContext.abort()` can be called to rollback the
   transaction.

`TransactionAwareHTable` handles the details of performing data operations transactionally, and
implements the necessary hooks in order to commit and rollback the data changes (see
`TransactionAware`).

### Example

To demonstrate how you might use `TransactionAwareHTable`\s, below is a basic implementation of a
`SecondaryIndexTable`. This class encapsulates the usage of a `TransactionContext` and provides a simple interface
to a user:

```java
  /**
   * A Transactional SecondaryIndexTable.
   */
  public class SecondaryIndexTable {
    private byte[] secondaryIndex;
    private TransactionAwareHTable transactionAwareHTable;
    private TransactionAwareHTable secondaryIndexTable;
    private TransactionContext transactionContext;
    private final TableName secondaryIndexTableName;
    private static final byte[] secondaryIndexFamily =
      Bytes.toBytes("secondaryIndexFamily");
    private static final byte[] secondaryIndexQualifier = Bytes.toBytes('r');
    private static final byte[] DELIMITER  = new byte[] {0};

    public SecondaryIndexTable(TransactionServiceClient transactionServiceClient,
                               HTable hTable, byte[] secondaryIndex) {
      secondaryIndexTableName =
            TableName.valueOf(hTable.getName().getNameAsString() + ".idx");
      HTable secondaryIndexHTable = null;
      HBaseAdmin hBaseAdmin = null;
      try {
        hBaseAdmin = new HBaseAdmin(hTable.getConfiguration());
        if (!hBaseAdmin.tableExists(secondaryIndexTableName)) {
          hBaseAdmin.createTable(new HTableDescriptor(secondaryIndexTableName));
        }
        secondaryIndexHTable = new HTable(hTable.getConfiguration(),
                                          secondaryIndexTableName);
      } catch (Exception e) {
        Throwables.propagate(e);
      } finally {
        try {
          hBaseAdmin.close();
        } catch (Exception e) {
          Throwables.propagate(e);
        }
      }

      this.secondaryIndex = secondaryIndex;
      this.transactionAwareHTable = new TransactionAwareHTable(hTable);
      this.secondaryIndexTable = new TransactionAwareHTable(secondaryIndexHTable);
      this.transactionContext = new TransactionContext(transactionServiceClient,
                                                       transactionAwareHTable,
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

        ArrayList<Get> gets = new ArrayList<Get>();
        for (Result result : indexScanner) {
          for (Cell cell : result.listCells()) {
            gets.add(new Get(cell.getValue()));
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
        ArrayList<Put> secondaryIndexPuts = new ArrayList<Put>();
        for (Put put : puts) {
          List<Put> indexPuts = new ArrayList<Put>();
          Set<Map.Entry<byte[], List<KeyValue>>> familyMap = put.getFamilyMap().entrySet();
          for (Map.Entry<byte [], List<KeyValue>> family : familyMap) {
            for (KeyValue value : family.getValue()) {
              if (value.getQualifier().equals(secondaryIndex)) {
                byte[] secondaryRow = Bytes.add(value.getQualifier(),
                                                DELIMITER,
                                                Bytes.add(value.getValue(),
                                                DELIMITER,
                                                value.getRow()));
                Put indexPut = new Put(secondaryRow);
                indexPut.add(secondaryIndexFamily, secondaryIndexQualifier, put.getRow());
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
  }
```

## Known Issues and Limitations

- Currently, column family `Delete` operations are implemented by writing a cell with an empty
  qualifier (empty `byte[]`) and empty value (empty `byte[]`). This is done in place of
  native HBase `Delete` operations so the delete marker can be rolled back in the event of
  a transaction failure -- normal HBase `Delete` operations cannot be undone. However, this
  means that applications that store data in a column with an empty qualifier will not be able to
  store empty values, and will not be able to transactionally delete that column.
- Column `Delete` operations are implemented by writing a empty value (empty `byte[]`) to the
  column. This means that applications will not be able to store empty values to columns.
- Invalid transactions are not automatically cleared from the exclusion list. When a transaction is
  invalidated, either from timing out or being invalidated by the client due to a failure to rollback
  changes, its transaction ID is added to a list of excluded transactions. Data from invalidated
  transactions will be dropped by the `TransactionProcessor` coprocessor on HBase region flush
  and compaction operations. Currently, however, transaction IDs can only be manually removed
  from the list of excluded transaction IDs, using the `org.apache.tephra.TransactionAdmin` tool.