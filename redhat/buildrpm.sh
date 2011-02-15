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

set -x
set -e

if [ -z "${BZR_REVISION}" ]; then
    # usually hudson sets this
    # figure out the build number outside of hudson (for user builds)
    BZR_REVISION=$(bzr revision-info | sed "s/\([0-9]*\) .*/\1/")
fi

TEAMCITY=${TEAMCITY:-0}
PUBLISH=${PUBLISH:-0}

rpm_env()
{
	sudo /usr/sbin/groupadd --force mockbuild
	sudo /usr/sbin/useradd -g mockbuild mockbuild || echo "Could not create user mockbuild, user may already exist"
}


# prepare source tarballs for rpm build to consume
tarball()
{
	local name=akserver
	local randir=/tmp/${RANDOM}
	local cdir=${randir}/${name}
	rm -rf ${cdir} rpmbuild ../target ${name}.tar.gz
	mkdir -p ${cdir}
	cp -r ../*        ${cdir}
	find ${cdir} -name .svn | xargs rm -rf
	tar -C ${randir} -cf ${name}.tar . 
	gzip ${name}.tar
	mkdir -p rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
	mv ${name}.tar.gz rpmbuild/SOURCES
	rm -rf ${randir}
    # replace only on line 9 of the file. rpm requires you to explictly set the release
	cat akiban-server.spec        | sed "9,9 s/REVISION/${BZR_REVISION}/g" > akiban-server-${BZR_REVISION}.spec 
}


# create chunkserver rpm
chunkserver_rpm()
{
    export BZR_REVISION 
	rpmbuild --target=noarch --define "_topdir ${PWD}/rpmbuild"   -ba akiban-server-${BZR_REVISION}.spec 
}

#update the yum repository
publish()
{
if [ ${PUBLISH} -gt 0 ];then
	ssh skeswani@172.16.20.117  " mkdir -p /var/www/rpms/akiban-server/${BZR_REVISION}"
	scp -r rpmbuild/RPMS/noarch/*.rpm   skeswani@172.16.20.117:/var/www/rpms/akiban-server/${BZR_REVISION}
	ssh skeswani@172.16.20.117 "rm -f /var/www/latest/* && ln -sf /var/www/rpms/akiban-server/${BZR_REVISION}/* /var/www/latest/"
	ssh skeswani@172.16.20.117 /var/www/rpms/createrepo.sh
	ssh -i ~/.ssh/akibanweb ubuntu@50.16.188.89 "mkdir -p /var/www/rpms/akiban-server/${BZR_REVISION}"
	scp -i ~/.ssh/akibanweb -r rpmbuild/RPMS/noarch/*.rpm ubuntu@50.16.188.89:/var/www/rpms/akiban-server/${BZR_REVISION}
	ssh -i ~/.ssh/akibanweb ubuntu@50.16.188.89 /var/www/rpms/createrepo.sh
fi
}

rpm_env
tarball
chunkserver_rpm
publish
