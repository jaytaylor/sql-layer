#!/bin/bash
# Parameters 
# $1 - DDL for the user tables, including the Akiban grouping comment
# $2 - output file
# $3 - output format.  see DDLSource --help for more info.
usage="usage: aisgen.sh <input-file> <output-file> <format>"
if [ -z "${1}" ] ; then
    echo ${usage}
    exit 1
fi

if [ "${1}" = "--help" ] ; then
java -cp ../../../../common/ais/target/akiban-ais-1.0.0-jar-with-dependencies.jar \
    com.akiban.ais.ddl.DDLSource --help
    exit $?
fi    
	

if [ -z "${2}" ] ; then
    echo ${usage}
    exit 1
fi

if [ -z "${3}" ] ; then
    echo ${usage}
    exit 1
fi

java -cp ../../../../common/ais/target/akiban-ais-1.0.0-jar-with-dependencies.jar \
    com.akiban.ais.ddl.DDLSource --input-file=${1} --output-file=${2} \
    --format=${3}
if [ $? -ne 0 ] ; then
    echo "aisgen.sh WARN: DDLSource returned an error"
fi

