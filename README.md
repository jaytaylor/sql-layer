# Welcome to the FoundationDB SQL Layer #

## 1. Overview ##

The SQL Layer is a scalable, fault tolerant, ANSI SQL engine, built as an open-source Layer on top of the [FoundationDB storage substrate](https://foundationdb.com/features). It provides the same high performance, multi-node scalability, fault-tolerance, and true multi-key ACID transactions, alongside higher level capabilities that include direct object access and a sophisticated SQL environment. It was written from the ground up in Java and utilizes the [FoundationDB SQL Parser](https://github.com/FoundationDB/sql-parser).

If you're looking for the SQL Layer documentation check out https://foundationdb.com/layers/sql.

## 2. Prerequisites ##

### Java ###

The FoundationDB SQL Layer requires the Java 7 Runtime Environment (JRE). Both OpenJDK and the Oracle JDK are supported.

### FoundationDB ###

To install the FoundationDB Storage substrate [download and follow the directions in the docs](https://foundationdb.com/get), and verify that the FoundationDB cluster is up and running (see 'Testing your FoundationDB installation' sections in the [getting started guides](https://foundationdb.com/documentation/getting-started.html)).

## 3. Install the SQL Layer From Packages ##

[Installers](https://foundationdb.com/layers/sql/documentation/GettingStarted/installation.html) are available for Mac OS X, Windows, Ubuntu and REHL/CentOS.

## 4. Alternatively, build FoundationDB SQL Layer From Source ##

Use [Maven](http://maven.apache.org) to build the project:

    $ mvn package

All unit and integration tests will be run by default, which could be lengthy. Test execution can be avoided with the `skipTests` option:

    $ mvn package -DskipTests=true

An executable jar, and required dependencies, will be the `target/` directory once packaging is complete.

The server can then be started with the `fdbsqllayer` script. The `-f` flag will run it in the foreground:

    $ ./bin/fdbsqllayer -f

A handful of informational messages will print and then the server will state it is ready:

    2013-03-22 15:36:29,561 [main] INFO  ServiceManager - FoundationDB SQL Layer x.y.z.rev ready.

## 5. Testing your SQL Layer installation ##

The SQL Layer can then be accessed though the SQL interface using the [fdbsqlcli](https://foundationdb.com/layers/sql/documentation/Admin/fdbsqlcli.html) command line utility

And through a RESTful API on port `8091`:

    $ curl http://localhost:8091/v1/version
    [
    {"server_name":"FoundationDB SQL Layer","server_version":"x.y.z.rev"}
    ]
    
And using SQL on port `15432` :

             _SQL_COL_1
    -----------------------------
     FoundationDB 1.9.4 +18dbac1
    (1 row)

## More Information ##

For questions and more information, visit our community site at http://community.foundationdb.com, the [google group](https://groups.google.com/forum/#!forum/foundationdb-user), or hop on the `#foundationdb` IRC channel on irc.freenode.net