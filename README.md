# FoundationDB SQL Layer #

> **IMPORTANT NOTICE: This repository is undergoing significant changes and is not ready for your use.**
>	
> The FoundationDB SQL Layer is being transitioned for use exclusivley with the FoundationDB storage substrate. The codebase currently uses a local btree by default and is used only for experimental purposes. If you'd like to be informed once the Layer is ready for primetime, please watch this repo, or sign up at https://foundationdb.com/layers/sql.

## 1. Overview ##

The FoundationDB SQL layer is a full SQL implementation that builds upon [FoundationDB’s storage substrate core properties](https://foundationdb.com/features). It provides the same high performance, multi-node scalability, fault-tolerance, and true multi-key ACID transactions, alongside higher level capabilities that include direct object access and a sophisticated SQL environment. It was written from the ground up in Java and utilizes the [FoundationDB SQL Parser](https://github.com/FoundationDB/sql-parser).

## 2. Prerequisites ##

The FoundationDB SQL Layer requires Java 7 Runtime Environment and FoundationDB.

### JRE ###

The FoundationDB SQL Layer requires the Java 7 Runtime Environment (JRE). Both OpenJDK and the Oracle JDK are supported but for production deployments, we recommend installing Oracle's JDK. Oracle Java can be downloaded from Oracle's website - http://java.com/en/download/manual.jsp?locale=en

### FoundationDB ###

To install the FoundationDB Storage substrate please follow the [directions in the docs](https://foundationdb.com/documentation/getting-started.html), and verify that the FoundationDB cluster is up and running (see 'Testing your FoundationDB installation' sections in the differnet getting started guides). Please make sure the FoundationDB cluster is running before moving any further.


## 3. Getting the SQL Layer


### a. Building From Source

Use [Maven](http://maven.apache.org) to build the project:

    $ mvn package

All unit and integration tests will be run by default, which could be lengthy. Test execution can be avoided with the `skipTests` option:

    $ mvn package -DskipTests=true

An executable jar, and required dependencies, will be the `target/` directory once packaging is complete.

The server can then be started with the `fdbsqllayer` script. The `-f` flag will run it in the foreground:

    $ ./bin/fdbsqllayer -f

A handful of informational messages will print and then the server will state it is ready:

    2013-03-22 15:36:29,561 [main] INFO  ServiceManager - FoundationDB SQL Layer ready.


### b. Installing From Packages

Official packages for Windows, OS X, Ubuntu and CentOS/RedHat are available.
See [Getting Started - Installing the SQL Layer](https://foundationdb.com/layers/sql/GettingStarted/getting.started.html)
for more details.

By default a single instance, appropriate for local development, of the
SQL Layer is installed. Consult the documentation for recommended multi-node
configurations.


## 5. Checking Your SQL Layer

The SQL Layer can then be accessed through a RESTful API on port `8091`:

    $ curl http://localhost:8091/v1/version
    [
    {"server_name":"FoundationDB SQL Layer","server_version":"x.y.z+hash"}
    ]
    
And the SQL client on port `15432`:

    $ fsql -c 'SELECT VERSION();'
            _SQL_COL_1         
    --------------------------
     FoundationDB x.y.z +hash 
    (1 row)


## Contact

* GitHub: http://github.com/FoundationDB/sql-layer
* Community: https://foundationdb.com/community
* IRC: #FoundationDB on irc.freenode.net

