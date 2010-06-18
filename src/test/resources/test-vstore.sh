#!/bin/bash
#pushd ../sql >> /dev/null

./aisgen.sh toy_schema.ddl toy_schema.sql sql
mysql -u root <../../../../common/ais/src/main/resources/akiba_information_schema.sql
mysql -u root <./toy_schema_test.sql
mysql -u root <./toy_schema_data.sql
