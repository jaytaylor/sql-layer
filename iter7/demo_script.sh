#!/bin/bash

export PREPAKIBA=/home/peter/bin/prepakiba
export TRUNK=/home/peter/akiba_akiba/trunk

echo Stop any left over CServer instances
pkill -f "cserver-1.0-SNAPSHOT-jar-with-dependencies.jar"
sleep 1
echo Start or restart the  ChunkServer
mkdir /tmp/chunkserver_data
rm /tmp/chunkserver_data/*
java $1 -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=8000,suspend=n,server=y -Xmx512M -jar $TRUNK/cserver/iter7/target/cserver-1.0-SNAPSHOT-jar-with-dependencies.jar localhost akiba akibaDB akiba_information_schema localhost 33060 &

sleep 3

echo Prepare MySQL with AkibaDB engine
sudo $PREPAKIBA

sleep 3

echo Load data dictionary
pushd $TRUNK/common/ais/
mysql -u root < src/test/resources/data_dictionary_test_schema.sql
popd

echo Bounce MySQL so to cause AIS to load
sudo service mysql restart

sleep 5

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
