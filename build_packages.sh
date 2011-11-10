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

if [ $# -lt 1 ]; then
    echo "Usage: ./build_packages.sh [debian|redhat]"
    exit 1
fi

platform=$1
bzr_revno=`bzr revno`

if [ -z "$2" ] ; then
	epoch=`date +%s`
else
	epoch=$2
fi

if [ ${platform} == "debian" ]; then
    cp packages-common/* ${platform}
    mvn -Dmaven.test.skip=true clean install -DBZR_REVISION=${bzr_revno}
    debuild
elif [ ${platform} == "redhat" ]; then
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    tar_file=${PWD}/redhat/rpmbuild/SOURCES/akserver.tar
    bzr export --format=tar $tar_file
    cp packages-common/* ${PWD}/redhat/akserver/redhat
    pushd redhat
# bzr st -S outs lines like "? redhat/akserver/redhat/log4j.properties"
# we want to turn those to just "akserver/redhat/log4j.properties"
    for to_add in $(bzr st -S . | sed 's/\?\s\+redhat\///'); do
        tar --append -f $tar_file $to_add
    done
    popd
    gzip $tar_file
    cat ${PWD}/redhat/akiban-server.spec | sed "9,9s/REVISION/${bzr_revno}/g" > ${PWD}/redhat/akiban-server-${bzr_revno}.spec
    sed -i "10,10s/EPOCH/${epoch}/g" ${PWD}/redhat/akiban-server-${bzr_revno}.spec
    rpmbuild --target=noarch --define "_topdir ${PWD}/redhat/rpmbuild" -ba ${PWD}/redhat/akiban-server-${bzr_revno}.spec
else
    echo "Invalid Argument: ${platform}"
    echo "Usage: ./build_packages.sh [debian|redhat]"
    exit 1
fi
