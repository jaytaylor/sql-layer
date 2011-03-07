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
    BZR_REVISION=$(bzr revno)
fi

TEAMCITY=${TEAMCITY:-0}
PUBLISH=${PUBLISH:-0}

if [ ${PUBLISH} -gt 0 ];then
	BZR_REVISION=$( ssh skeswani@172.16.20.117  "/var/www/rpms/reserve_version.sh /var/www/rpms/akiban-server ${BZR_REVISION}" )
fi

rpm_env()
{
	sudo /usr/sbin/groupadd --force mockbuild
	sudo /usr/sbin/useradd -g mockbuild mockbuild || echo "Could not create user mockbuild, user may already exist"
}

# create server rpm
server_rpm()
{
    export BZR_REVISION 
	cat akiban-server.spec | sed "9,9 s/REVISION/${BZR_REVISION}/g" > akiban-server-${BZR_REVISION}.spec 
    mkdir -p ${PWD}/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    bzr export --format=tgz ${PWD}/rpmbuild/SOURCES/akserver.tar.gz
	rpmbuild --target=noarch --define "_topdir ${PWD}/rpmbuild" -ba akiban-server-${BZR_REVISION}.spec 
}

#update the yum repository
publish()
{
if [ ${PUBLISH} -gt 0 ];then
	scp -r rpmbuild/RPMS/noarch/*.rpm   skeswani@172.16.20.117:/var/www/rpms/akiban-server/${BZR_REVISION}
	ssh skeswani@172.16.20.117 "rm -f /var/www/latest/* && ln -sf /var/www/rpms/akiban-server/${BZR_REVISION}/* /var/www/latest/"
	ssh skeswani@172.16.20.117 /var/www/rpms/createrepo.sh
	ssh -i ~/.ssh/akibanweb ubuntu@50.16.188.89 "mkdir -p /var/www/rpms/akiban-server/${BZR_REVISION}"
	scp -i ~/.ssh/akibanweb -r rpmbuild/RPMS/noarch/*.rpm ubuntu@50.16.188.89:/var/www/rpms/akiban-server/${BZR_REVISION}
	ssh -i ~/.ssh/akibanweb ubuntu@50.16.188.89 /var/www/rpms/createrepo.sh
fi
}

rpm_env
server_rpm
publish
