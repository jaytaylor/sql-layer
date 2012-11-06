#!/bin/bash
TARGET=$(ls -d $(dirname $0)/../../../target)
BASEJAR=$(ls ${TARGET}/akiban-server-*.*.*-SNAPSHOT.jar)
java -cp "target/test-classes:${BASEJAR}:${BASEJAR%.jar}-tests.jar:${TARGET}/dependency/*:/usr/share/java/postgresql.jar:/usr/share/java/mysql-connector-java.jar" com.akiban.sql.test.SQLClient "$@"
