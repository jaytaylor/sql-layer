# FoundationDB SQL Layer #

> **IMPORTANT NOTICE: This repository is undergoing significant changes and is not ready for your use.**
>	
> The FoundationDB SQL Layer is being transitioned for use exclusivley with the FoundationDB storage substrate. The codebase currently uses a local btree by default and is used only for experimental purposes. If you'd like to be informed once the Layer is ready for primetime, please watch this repo, or sign up at https://foundationdb.com/layers/sql.

## 1. Overview ##

The FoundationDB SQL layer is a full SQL implementation that builds upon [FoundationDB’s storage substrate core properties](https://foundationdb.com/features). It provides the same high performance, multi-node scalability, fault-tolerance, and true multi-key ACID transactions, alongside higher level capabilities that include direct object access and a sophisticated SQL environment. It was written from the ground up in Java and utilizes the [FoundationDB SQL Parser](https://github.com/FoundationDB/sql-parser).

## 2. Prerequisites ##

The FoundationDB SQL Layer requires Java 7 Runtime Environment, a postgreSQL client, and FoundationDB.

### Java ###

The FoundationDB SQL Layer requires the Java 7 Runtime Environment (JRE). Both OpenJDK and the Oracle JDK are supported but for production deployments, we recommend installing Oracle's JDK. Oracle Java can be downloaded from Oracle's website - http://java.com/en/download/manual.jsp?locale=en

### PostgreSQL Client ###

The `psql` command line utility for PostgreSQL is used for connecting 
to the FoundationDB SQL Layer beacause the layer natively speaks the PostgreSQL protocol. 

On CentOS use the official PostgreSQL yum repositories::

  rpm -ivh http://yum.pgrpms.org/8.1/redhat/rhel-5-x86_64/postgresql-libs-8.1.23-1PGDG.rhel5.x86_64.rpm
  rpm -ivh http://yum.pgrpms.org/8.1/redhat/rhel-5-x86_64/postgresql-8.1.23-1PGDG.rhel5.x86_64.rpm

On ubuntu simply type: 

  sudo apt-get install postgresql-client
  
As of version 10.7, Mac OS X Ships with the required drivers and command line utility, so no action is required. Clients for earlier version of OS X (Lion and Snow Leopard 64bit only) should [download and install the PostgreSQL_Client-9.1.4-1.dmg file](http://www.kyngchaos.com/files/software/postgresql/PostgreSQL_Client-9.1.4-1.dmg).
  
For windows use the psql client that comes bundled with the [PostgreSQL installer](http://www.postgresql.org)

### FoundationDB ###

To install the FoundationDB Storage substrate please follow the [directions in the docs](https://foundationdb.com/documentation/getting-started.html), and verify that the FoundationDB cluster is up and running (see 'Testing your FoundationDB installation' sections in the differnet getting started guides). Please make sure the FoundationDB cluster is running before moving any further.

## 3. Building FoundationDB SQL Layer From Source ##

Use [Maven](http://maven.apache.org) to build the project:

    $ mvn package

All unit and integration tests will be run by default, which could be lengthy. Test execution can be avoided with the `skipTests` option:

    $ mvn package -DskipTests=true

An executable jar, and required dependencies, will be the `target/` directory once packaging is complete.

The server can then be started with the `fdbsqllayer` script. The `-f` flag will run it in the foreground:

    $ ./bin/fdbsqllayer -f

A handful of informational messages will print and then the server will state it is ready:

    2013-03-22 15:36:29,561 [main] INFO  ServiceManager - FoundationDB SQL Layer x.y.z.rev ready.

## 4. Alternatively, Install the SQL Layer From Packages ##

Packages for Debian/Ubuntu and Centos/RedHat will be avaliable shortly on https://foundationdb.com/layers/sql.

Using them on Ubuntu can be done as follows (replace x.y.z with appropriate version):
    
    $ sudo dpkg -i fdb-sql-layer_x.y.z_all.deb \ 
	fdb-sql-layer_x.y.z_all.deb

Similarly on CentOS:
    
    $ sudo rpm -Uvh fdb-sql-layer-x.y.z-r.noarch.rpm \
    fdb-sql-layer-x.y.z-r.noarch.rpm

Windows and Mac OSX installers will also be avaliable at https://foundationdb.com/layers/sql.

By default a single instance of the FoundationDB SQL Layer is installed, appropriate for a development workstation. Consult the documentation for recommended multi node configurations.

## 5. Testing your SQL Layer installation ##

The SQL Layer can then be accessed through a RESTful API on port `8091`:

    $ curl http://localhost:8091/v1/version
    [
    {"server_name":"FoundationDB SQL Layer","server_version":"x.y.z.rev"}
    ]
    
And the Postgres protocol on port `15432`:
	
    $ psql "host=localhost port=15432" -c 'SELECT * FROM information_schema.server_instance_summary'
      server_name           | server_version
    ------------------------+-----------------
     FoundationDB SQL Layer | x.y.z.rev
    (1 row)

## Contact

* GitHub: http://github.com/FoundationDB/sql-layer
* Community: https://foundationdb.com/community
* IRC: #FoundationDB on irc.freenode.net

