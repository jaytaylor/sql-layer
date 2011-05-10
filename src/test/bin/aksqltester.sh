#!/bin/bash
BASEJAR=$(ls $(dirname $0)/../../../target/akiban-server-*.*.*-SNAPSHOT.jar)
java -cp java -cp ${BASEJAR%.jar}-jar-with-dependencies.jar:${BASEJAR%.jar}-tests.jar com.akiban.sql.test.Tester "$@"
