#!/bin/bash
BASEJAR=$(ls $(dirname $0)/../../../target/akiban-server-*.*.*-SNAPSHOT.jar)

java -cp target/test-classes:${BASEJAR%.jar}-jar-with-dependencies.jar:${BASEJAR%.jar}-tests.jar:/usr/share/java/postgresql-jdbc3.jar:/usr/share/java/mysql-connector-java.jar com.akiban.sql.test.SQLClient "$@"
