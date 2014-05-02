# FoundationDB SQL Layer

## Overview

The FoundationDB SQL layer is a full SQL implementation that builds on
[FoundationDB’s core features](https://foundationdb.com/features). It gets
the same high performance, multi-node scalability, fault-tolerance, and
true multi-key ACID transactions while also providing new capabilities,
including a sophisticated SQL environment and direct object access.

It was written from the ground up in Java and utilizes the
[FoundationDB SQL Parser](https://github.com/FoundationDB/sql-parser).

Check out https://foundationdb.com/layers/sql for the full documentation.

## Prerequisites

The FoundationDB SQL Layer requires Java 7 Runtime Environment and FoundationDB.

### JRE

The FoundationDB SQL Layer requires the Java 7 Runtime Environment (JRE). Both
OpenJDK JRE and the official Oracle JRE are supported.

See the [Oracle Java SE Downloads](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
page for installation details.


### FoundationDB

To install FoundationDB, follow the official [Getting Started](https://foundationdb.com/documentation/getting-started.html)
guides, being sure to check that the cluster is up and running (covered
in *Testing your FoundationDB installation*).


## Installing the SQL Layer

The SQL Layer can be installed from system packages (appropriate for most
people) or directly from source (intended for developers).

### a. Packages

Official packages for Windows, OS X, Ubuntu and CentOS/RedHat are available.
See the [installation section](https://foundationdb.com/layers/sql/documentation/GettingStarted/installation.html)
for more details.

A single instance of the SQL Layer is installed by default and is appropriate
for local development. Consult the documentation for recommended multi-node
configurations.

### b. Source

Note: This section is intended *only* for developers.

Use [Maven](http://maven.apache.org) to build the project:

    $ mvn package

All unit and integration tests will be run by default, which could be lengthy.
Test execution can be avoided with the `skipTests` option:

    $ mvn package -DskipTests=true

An executable jar, and required dependencies, will be the `target/` directory
once packaging is complete.

The server can then be started with the `fdbsqllayer` script. The `-f` flag
will run it in the foreground:

    $ ./bin/fdbsqllayer -f

A handful of informational messages will print and then the server will state it is ready:

    2013-03-22 15:36:29,561 [main] INFO  ServiceManager - FoundationDB SQL Layer ready.

When installing from source, you'll also want the
[SQL Layer Client Tools](https://github.com/FoundationDB/sql-layer-client-tools).


## 5. Testing Your SQL Layer Installation

The SQL Layer can then be accessed using the SQL client on port `15432`:

    $ fdbsqlcli -c 'SELECT VERSION();'
            _SQL_COL_1         
    --------------------------
     FoundationDB x.y.z +hash 
    (1 row)

And using a RESTful API on port `8091`:

    $ curl http://localhost:8091/v1/version
    [
    {"server_name":"FoundationDB SQL Layer","server_version":"x.y.z+hash"}
    ]

## Contact

* GitHub: http://github.com/FoundationDB/sql-layer
* Community: https://foundationdb.com/community
* IRC: #FoundationDB on irc.freenode.net
