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