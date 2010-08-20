#/bin/bash
set -x

# --cserver localhost:8080
# --mysql   localhost
# --user    root
# --temp    bulkload
# --group   coi
# --source  acme:acme_source

CLASSPATH=CSERVER.INSTALL.DIR/usr/local/chunkserver/akiban-cserver-1.0-SNAPSHOT-jar-with-dependencies.jar

java  -ea -cp ${CLASSPATH} com.akiban.cserver.util.BulkLoaderClient  $* 
