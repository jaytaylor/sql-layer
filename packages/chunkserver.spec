Name: chunkserver 
Version: 0.0.2
Release: REVISION%{?dist}
Summary: chunkserver
Group: Applications/storage-server
URL: http://www.akiban.com
License: GPLv2 with exceptions

Source0: cserver.tar.gz

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
Prereq: /sbin/ldconfig, /sbin/install-info, grep, fileutils
#BuildRequires: ant >= 1.7,mvn >= 2.2.1, java >= 1.6.0.17
BuildRequires: bash java > 1.6
Requires: bash java >= 1.6.


%description
chunkserver

%prep
%setup -q -n cserver

%build
mvn -B clean install -Dmaven.test.skip=true 

%install

rm -rf ${RPM_BUILD_ROOT}
ant -f install.xml -Dcserver.install.dir=${RPM_BUILD_ROOT} -Dmysql.install.dir=/usr -Dcserver.prefix="" 


%clean
rm -rf ${RPM_BUILD_ROOT}

%pre

%post 

%postun

%files
%defattr(-,root,root)
# file manifest
# rpm build needs to know all files and dirs in the package, no less no more!
# 
# fequent adding and removing files by developers is breaking the rpm build
# hence using uber/catch all directory names. this will make the build more reliable.
/etc/init.d/chunkserverd
/var/lib/chunkserver
/usr/local/chunkserver
/var/log
/var/run
/usr/local/chunkserver/akiban-cserver-0.0.2-SNAPSHOT-jar-with-dependencies.jar
/etc/akiban/config/ais_object_model.xml
/etc/akiban/config/cluster.properties
/etc/akiban/config/chunkserver.properties
/etc/akiban/config/log4j.properties
