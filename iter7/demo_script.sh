#!/bin/bash

# Reset database
sudo /home/peter/bin/prepakiba
# Load data dictionary
pushd /home/peter/akiba_akiba/trunk/common/ais/
mysql -u root < src/test/resources/data_dictionary_test_schema.sql
popd

# Start chunkserver

mkdir /tmp/data

pkill java
java -Xmx512M -jar /home/peter/akiba_akiba/trunk/cserver/iter7/target/cserver-1.0-SNAPSHOT-jar-with-dependencies.jar localhost akiba akibaDB akiba_information_schema localhost 33060  &

sleep 10
echo Hit enter to insert rows
read $fred

echo Insert some rows
mysql -u root <<EOF
use data_dictionary_test;
insert into \`order\` values(101, 1, 20100112);
insert into \`order\` values(102, 1, 20100112);
insert into \`order\` values(103, 1, 20100112);
insert into \`order\` values(104, 1, 20100112);
insert into \`order\` values(105, 1, 20100112);
insert into \`order\` values(201, 2, 20100112);
insert into \`order\` values(202, 2, 20100112);
insert into \`order\` values(203, 2, 20100112);
insert into \`order\` values(204, 1, 20100112);
insert into \`order\` values(204, 1, 20100112);
insert into customer values(1, 'First Customer');
insert into customer values(2, 'Second Customer');
insert into customer values(3, 'Third Customer');
insert into customer values(4, 'Fourth Customer');
insert into item values(101, 1, 1, 10101);
insert into item values(101, 2, 5, 10102);
insert into item values(101, 3, 1, 10103);
insert into item values(101, 4, 7, 10104);
insert into item values(101, 5, 1, 10105);
EOF
