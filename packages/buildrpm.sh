#!/bin/bash
set -x
set -e

TEAMCITY=${TEAMCITY:-0}
PUBLISH=${PUBLISH:-0}
rev=${BUILD_VCS_NUMBER:-1000}

rpm_env()
{
	sudo /usr/sbin/groupadd --force mockbuild
	sudo /usr/sbin/useradd -g mockbuild mockbuild || echo "Could not create user mockbuild, user may already exist"
}


# prepare source tarballs for rpm build to consume
tarball()
{
	local name=cserver
	local randir=/tmp/${RANDOM}
	local cdir=${randir}/${name}
	rm -rf ${cdir} rpmbuild ../target
	mkdir -p ${cdir}
	cp -r ../*        ${cdir}
	cp -r ../../config        ${randir}
	find ${cdir} -name .svn | xargs rm -rf
	tar -C ${randir} -cf ${name}.tar . 
	gzip ${name}.tar
	mkdir -p rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
	mv ${name}.tar.gz rpmbuild/SOURCES
	cat chunkserver.spec        | sed "s/REVISION/${rev}/g" > chunkserver-${rev}.spec 
}


# create chunkserver rpm
chunkserver_rpm()
{
	rpmbuild --target=noarch --define "_topdir ${PWD}/rpmbuild"   -ba chunkserver-${rev}.spec 
}

#update the yum repository
publish()
{
if [ ${PUBLISH} -gt 0 ];then
	ssh skeswani@172.16.20.117  " mkdir -p /var/www/rpms/${rev}"
	scp -r rpmbuild/RPMS/noarch/*.rpm   skeswani@172.16.20.117:/var/www/rpms/${rev}
	ssh skeswani@172.16.20.117 "rm -f /var/www/latest/* && ln -sf /var/www/rpms/${rev}/* /var/www/latest/"
	ssh skeswani@172.16.20.117 /var/www/rpms/createrepo.sh
fi
}

rpm_env
tarball
chunkserver_rpm
publish
