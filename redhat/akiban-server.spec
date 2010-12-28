%define __jar_repack %{nil}

%global username akiban

%define relname %{name}-%{version}-%{release}

Name:           akiban-server
Version:        0.0.2
Release:        rc3
Summary:        Akiban Server is the main server for the Akiban Orthogonal Architecture.

Group:          Development/Libraries
License:        GPLv2 with exceptions
URL:            http://akiban.com/
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils

Conflicts:     akiban-server

Requires:      java >= 1.6.0
Requires:      jpackage-utils
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
%setup -q -n cserver

%build
mvn -Dmaven.test.skip=true -B clean install

%install
%{__rm} -rf %{buildroot}
mkdir -p %{buildroot}%{_sysconfdir}/%{username}/
mkdir -p %{buildroot}/usr/share/%{username}
mkdir -p %{buildroot}/usr/share/%{username}/lib
mkdir -p %{buildroot}/etc/%{username}
mkdir -p %{buildroot}/etc/rc.d/init.d/
mkdir -p %{buildroot}/etc/security/limits.d/
mkdir -p %{buildroot}/etc/default/
mkdir -p %{buildroot}/usr/sbin
mkdir -p %{buildroot}/usr/bin
cp -p redhat/cluster.properties %{buildroot}/etc/%{username}/
cp -p redhat/chunkserver.properties %{buildroot}/etc/%{username}/
cp -p redhat/log4j.properties %{buildroot}/etc/%{username}/
cp -p redhat/akiban-server %{buildroot}/etc/rc.d/init.d/
cp -p target/akiban-cserver-0.0.2-SNAPSHOT-jar-with-dependencies.jar %{buildroot}/usr/share/%{username}/lib
cp conf/akiban-env.sh %{buildroot}/etc/%{username}/
mv bin/akserver %{buildroot}/usr/sbin
mkdir -p %{buildroot}/var/lib/%{username}
mkdir -p %{buildroot}/var/lib/%{username}
mkdir -p %{buildroot}/var/lib/%{username}
mkdir -p %{buildroot}/var/run/%{username}
mkdir -p %{buildroot}/var/log/%{username}

%clean
%{__rm} -rf %{buildroot}

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
%doc CHANGES.txt LICENSE.txt README.txt NEWS.txt NOTICE.txt
%attr(755,root,root) %{_sbindir}/akserver
%attr(755,root,root) /etc/rc.d/init.d/akiban-server
%attr(755,root,root) /etc/default/%{username}
%attr(755,%{username},%{username}) /usr/share/%{username}*
%attr(755,%{username},%{username}) %config(noreplace) /%{_sysconfdir}/%{username}
%attr(755,%{username},%{username}) %config(noreplace) /var/lib/%{username}/*
%attr(755,%{username},%{username}) /var/log/%{username}*
%attr(755,%{username},%{username}) /var/run/%{username}*

%post
alternatives --install /etc/%{username}/conf %{username} /etc/%{username}/default.conf/ 0
exit 0

%postun
# only delete alternative on removal, not upgrade
if [ "$1" = "0" ]; then
    alternatives --remove %{username} /etc/%{username}/default.conf/
fi
exit 0

%changelog
* Tue Aug 03 2010 Nick Bailey <nicholas.bailey@rackpace.com> - 0.7.0-1
- Updated to make configuration easier and changed package name.
* Mon Jul 05 2010 Peter Halliday <phalliday@excelsiorsystems.net> - 0.6.3-1
- Initial package
