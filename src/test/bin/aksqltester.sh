#!/bin/bash
TARGET=$(ls -d $(dirname $0)/../../../target)
BASEJAR=$(ls ${TARGET}/akiban-server-*.*.*-SNAPSHOT.jar)
java -cp "${BASEJAR}:${BASEJAR%.jar}-tests.jar:${TARGET}/dependency/*" com.akiban.sql.test.Tester "$@"
