<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

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

Please refer to the [Getting Started](http://tephra.incubator.apache.org/GettingStarted.html) guide to
start using [Apache Tephra](http://tephra.incubator.apache.org).

## Building
You can build Tephra directly from the latest source code:

```sh
  git clone https://git-wip-us.apache.org/repos/asf/incubator-tephra.git
  cd incubator-tephra
  mvn clean package
```
