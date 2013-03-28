Akiban Server
=============

Overview
--------

Akiban Server is a new relational database that offers document and SQL style access while eliminating the cost of joins.

It was written from the ground up in Java and utilizes [Persistit](https://github.com/akiban/persistit) and the [Akiban SQL Parser](https://github.com/akiban/sql-parser).


Building Akiban Server From Source
----------------------------------

Use [Maven](http://maven.apache.org) to build the project:

    $ mvn package

All unit and integration tests will be run by default, which could be lengthy. Test execution can be avoided with the `skipTests` option:

    $ mvn package -DskipTests=true

An executable jar, and required dependencies, will be the `target/` directory once packaging is complete.

The server can then be started with the `akserver` script. The `-f` flag will run it in the foreground:

    $ ./bin/akserver -f

A handful of informational messages will print and then the server will state it is ready:

    2013-03-22 15:36:29,561 [main] INFO  ServiceManager - Akiban Server x.y.z.rev ready.

The server can then be accessed over a RESTful API on port `8091` or the Postgres protocol on port `15432`:

    $ curl http://localhost:8091/v1/version
    [
    {"server_name":"Akiban Server","server_version":"x.y.z.rev"}
    ]
    
    $ psql "host=localhost port=15432" -c 'SELECT * FROM information_schema.server_instance_summary'
      server_name  | server_version
    ---------------+-----------------
     Akiban Server | x.y.z.rev
    (1 row)


Install Akiban Server Packages
------------------------------

Repositories for Debian/Ubuntu and Centos/RedHat are hosted on software.akiban.com.

Using them on Ubuntu is as simple as:
    
    $ sudo add-apt-repository "deb http://software.akiban.com/apt-public/ lucid main"
    $ sudo apt-get update
    $ sudo apt-get install akiban-server

CentOS is also straight forward:
    
    $ cat << EOF |sudo tee /etc/yum.repos.d/akiban.repo
    [akiban]
    name=akiban
    baseurl=http://software.akiban.com/releases
    gpgcheck=0
    enabled=1
    EOF
    $ sudo yum install akiban-server

Visit [software.akiban.com/releases](http://software.akiban.com/releases) directly for manual download and installation.


More Information
----------------

Documentation overview can be found at [akiban.com/docs.html](http://akiban.com/docs.html). Full documentation is hosted at [akiban.readthedocs.org](https://akiban.readthedocs.org/en/latest/) and generated from [akiban-docs](https://github.com/akiban/akiban-docs).

Join the [Akiban User](https://groups.google.com/a/akiban.com/d/forum/akiban-user) Google Group or find us on the **#akiban** channel on **irc.freenode.net**.

