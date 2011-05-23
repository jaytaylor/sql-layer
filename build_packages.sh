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


if [ $# -ne 1 ]; then
    echo "Usage: ./build_packages.sh [debian|redhat]"
    exit 1
fi

platform=$1
bzr_revno=`bzr revno`
cp packages-common/* ${platform}

if [ ${platform} == "debian" ]; then
    mvn -Dmaven.test.skip=true clean install -DBZR_REVISION=${bzr_revno}
    debuild
elif [ ${platform} == "redhat" ]; then
    cat ${PWD}/redhat/akiban-server.spec | sed "9,9s/REVISION/${bzr_revno}/g" > ${PWD}/redhat/akiban-server-${bzr_revno}.spec
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    bzr export --format=tgz ${PWD}/redhat/rpmbuild/SOURCES/akserver.tar.gz
    rpmbuild --target=noarch --define "_topdir ${PWD}/redhat/rpmbuild" -ba ${PWD}/redhat/akiban-server-${bzr_revno}.spec
else
    echo "Invalid Argument: ${platform}"
    echo "Usage: ./build_packages.sh [debian|redhat]"
    exit 1
fi
