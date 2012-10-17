%define __jar_repack %{nil}

%global username akiban

%define relname %{name}-%{version}-%{release}

Name:           akiban-server
Version:        1.4.3
Release:        REVISION%{?dist}
Epoch:		EPOCH
Summary:        Akiban Server is the main server for the Akiban Orthogonal Architecture.

Group:          Applications/Databases
License:        AGPL
URL:            http://akiban.com/

Source0:       akserver.tar.gz
BuildRoot:     %{_tmppath}/%{name}-%{version}-%{release}-root

Requires:      jre >= 1.6.0
Requires(pre): user(akiban)
Requires(pre): group(akiban)
Requires(pre): shadow-utils
Provides:      user(akiban)
Provides:      group(akiban)

BuildArch:      noarch

%description
Akiban Server is the main server for the Akiban Orthogonal Architecture.

For more information see http://akiban.com/

%prep
%setup -q -n akserver

%build
mvn -B -Dmaven.test.skip.exec -DBZR_REVISION=%{release} clean install

%install
rm -rf ${RPM_BUILD_ROOT}
mkdir -p ${RPM_BUILD_ROOT}%{_sysconfdir}/%{username}/
mkdir -p ${RPM_BUILD_ROOT}/usr/share/%{username}
mkdir -p ${RPM_BUILD_ROOT}/etc/%{username}/config
mkdir -p ${RPM_BUILD_ROOT}/etc/rc.d/init.d/
mkdir -p ${RPM_BUILD_ROOT}/etc/security/limits.d/
mkdir -p ${RPM_BUILD_ROOT}/etc/default/
mkdir -p ${RPM_BUILD_ROOT}/usr/sbin
mkdir -p ${RPM_BUILD_ROOT}/usr/bin

cp -p redhat/log4j.properties ${RPM_BUILD_ROOT}/etc/%{username}/config
cp -p redhat/server.properties ${RPM_BUILD_ROOT}/etc/%{username}/config
cp -p redhat/services-config.yaml ${RPM_BUILD_ROOT}/etc/%{username}/config
cp -p redhat/jvm.options ${RPM_BUILD_ROOT}/etc/%{username}/config
cp -p redhat/akiban-server ${RPM_BUILD_ROOT}/etc/rc.d/init.d/
cp -p redhat/LICENSE.txt ${RPM_BUILD_ROOT}/usr/share/%{username}
cp -p redhat/*.tag ${RPM_BUILD_ROOT}/usr/share/%{username}
cp -p redhat/postgresql.jar ${RPM_BUILD_ROOT}/usr/share/%{username}

cp -p target/akiban-server-1.4.3-SNAPSHOT-jar-with-dependencies.jar ${RPM_BUILD_ROOT}/usr/share/%{username}
ln -s /usr/share/%{username}/akiban-server-1.4.3-SNAPSHOT-jar-with-dependencies.jar ${RPM_BUILD_ROOT}/usr/share/%{username}/akiban-server.jar
cp -p redhat/akiban-client-tools-1.3.3-SNAPSHOT.jar ${RPM_BUILD_ROOT}/usr/share/%{username}
ln -s /usr/share/%{username}/akiban-client-tools-1.3.3-SNAPSHOT.jar ${RPM_BUILD_ROOT}/usr/share/%{username}/akiban-client-tools.jar

mv redhat/akdump ${RPM_BUILD_ROOT}/usr/bin
mv bin/akserver ${RPM_BUILD_ROOT}/usr/sbin

mkdir -p ${RPM_BUILD_ROOT}/var/lib/%{username}
mkdir -p ${RPM_BUILD_ROOT}/var/lib/%{username}
mkdir -p ${RPM_BUILD_ROOT}/var/lib/%{username}
mkdir -p ${RPM_BUILD_ROOT}/var/run/%{username}
mkdir -p ${RPM_BUILD_ROOT}/var/log/%{username}

%clean
rm -rf ${RPM_BUILD_ROOT}

%pre
getent group %{username} >/dev/null || groupadd -r %{username}
getent passwd %{username} >/dev/null || \
useradd -d /usr/share/%{username} -g %{username} -M -r %{username}
exit 0

%preun
# only delete user on removal, not upgrade
if [ "$1" = "0" ]; then
    userdel %{username}
fi

%files
%defattr(-,root,root,0755)
%attr(755,root,root) %{_sbindir}/akserver
%attr(755,root,root) %{_bindir}/akdump
%attr(755,root,root) /etc/rc.d/init.d/akiban-server
%attr(755,%{username},%{username}) /usr/share/%{username}*
%attr(755,%{username},%{username}) %config(noreplace) /%{_sysconfdir}/%{username}
%attr(755,%{username},%{username}) %config(noreplace) /var/lib/%{username}
%attr(755,%{username},%{username}) /var/log/%{username}*
%attr(755,%{username},%{username}) /var/run/%{username}*

%post
alternatives --install /etc/%{username}/config %{username} /etc/%{username}/default.conf/ 0
# make akiban start/shutdown automatically
if [ -x /sbin/chkconfig ]; then
    /sbin/chkconfig --add akiban-server
fi
exit 0

%postun
# only delete alternative on removal, not upgrade
if [ "$1" = "0" ]; then
    # stop akiban-server before uninstalling it
    if [ -x %{_sysconfdir}/init.d/akiban-server ]; then
        %{_sysconfdir}/init.d/akiban-server stop > /dev/null
        # don't start it automatically anymore
        if [ -x /sbin/chkconfig ]; then
            /sbin/chkconfig --del akiban-server
        fi
    fi
    alternatives --remove %{username} /etc/%{username}/default.conf/
fi
exit 0
