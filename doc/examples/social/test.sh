#!/bin/bash
#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#


case "$1" in
    rails)
        POST_MYSQL_URL="http://localhost:3000/home/post/"
        POST_AKIBAN_URL="http://localhost:3000/home/postm/"
        MYSQL_SCHEMA="social_rails_development"
        AKIBAN_SCHEMA="social_rails_mdevelopment"
        echo "Starting server"
        ./rails/script/server &> /dev/null &
        SERVER_PID="$!"
        ;;
    java|php)
        echo "Not yet implemented"
        exit 1
        ;;
    *)
        echo "usage: $0 <rails|php|java>" 1>&2
        exit 1
        ;;
esac

echo "Create MySQL schema"
mysql -u root -e "create schema $MYSQL_SCHEMA"
echo "Create MySQL tables"
mysql -u root $MYSQL_SCHEMA < schema.sql
echo "Load MySQL data"
mysql -u root $MYSQL_SCHEMA < data.sql

echo "Create Akiban schema"
mysql -u root -e "create schema $AKIBAN_SCHEMA"
echo "Create Akiban tables"
mysql -u root --init-command="set default_storage_engine=akibandb" $AKIBAN_SCHEMA < schema_grouped.sql
echo "Load Akiban data"
mysql -u root $AKIBAN_SCHEMA < data.sql

FAILURE="no"

echo "Compare post output "
for i in `seq 1 100`; do
    curl -s "$POST_MYSQL_URL""$i" > post_mysql.html
    curl -s "$POST_AKIBAN_URL""$i" > post_akiban.html
    diff post_mysql.html post_akiban.html > /dev/null
    if [ $? -ne 0 ]; then
        FAILURE="yes"
        echo -n "$i(x) "
    else
        echo -n "$i "
    fi
    rm -f post_mysql.html post_akiban.html
done
echo ""
echo "Done"

echo "Stopping server"
kill -9 "$SERVER_PID"
sleep 1

echo "Drop MySQL tables"
mysql -u root -e "drop schema $MYSQL_SCHEMA"
echo "Drop Akiban tables"
mysql -u root $AKIBAN_SCHEMA -e "drop table post_votes,post_tags,post_links,comments,posts,user_friends,users"
mysql -u root -e "drop schema $AKIBAN_SCHEMA"

if [ "$FAILURE" = "yes" ]; then
    echo "Failure" && exit 1
else
    echo "Success" && exit 0
fi
