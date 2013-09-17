#!/bin/bash
#
# Copyright (C) 2009-2013 FoundationDB, LLC
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

usage="Usage: ./build_packages.sh [--git-hash] [--git-count] [debian|redhat|macosx|binary] [... epoch]"
if [ $# -lt 1 ]; then
    echo "${usage}"
    exit 1
fi

platform=$1
git_hash=`git rev-parse --short HEAD`
git_count=`git rev-list --merges HEAD |wc -l |tr -d ' '` # --count is newer
layer_version=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep -o '^[0-9.]\+')

if [ "${1}" = "--git-hash" ]; then
    echo "${git_hash}"
    exit 0
fi

if [ "${1}" = "--git-count" ]; then
    echo "${git_count}"
    exit 0
fi


mvn_package="mvn clean package -B -U -DGIT_COUNT=${git_count} -DGIT_HASH=${git_hash} -DskipTests=true"

echo "Building FoundationDB SQL Layer"

mkdir -p target
mkdir -p packages-common/client
mkdir -p packages-common/plugins
common_dir="config-files/"
if [ ! -d ${common_dir} ]; then
    echo "fatal: Couldn't find configuration files in: ${common_dir}"
    exit 1
fi
echo "-- packages-common directory: ${common_dir} (Linux only)"
cp ${common_dir}/* packages-common/
cp bin/fdbsqllayer LICENSE.txt packages-common/

#
# Add client-tools
#
: ${TOOLS_LOC:="git@github.com:FoundationDB/sql-layer-client-tools.git"}
echo "Using client-tools location: ${TOOLS_LOC}"
pushd .
cd target
rm -rf client-tools
git clone ${TOOLS_LOC} client-tools
cd client-tools
mvn clean package -B -U -DskipTests=true
rm -f target/*-tests.jar target/*-sources.jar
cp bin/fdbsql{dump,load} ../../packages-common/
cp target/fdb-sql-layer-client-tools-*.jar ../../packages-common/
cp target/dependency/* ../../packages-common/client/
popd

if [ -z "$2" ] ; then
	epoch=`date +%s`
else
	epoch=$2
fi

# Handle platform-specific packaging process
if [ ${platform} == "debian" ]; then
    cp -R packages-common/* ${platform}
    ${mvn_package}
    mkdir -p ${platform}/server/
    cp ./target/dependency/* ${platform}/server/
    cp -R packages-common/plugins/ ${platform}/
    # No sign source, no sign changes, binary only
    debuild -us -uc -b
elif [ ${platform} == "redhat" ]; then
    rm -rf ${PWD}/redhat/rpmbuild
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    BUILD="${PWD}/redhat/rpmbuild/BUILD"
    ${mvn_package}
    cp -r packages-common/* target/ redhat/fdb-sql-layer.init "${BUILD}"
    spec_file="$PWD/redhat/fdb-sql-layer.spec"
    sed -e "s/_EPOCH/${epoch}/g" \
        -e "s/_GIT_COUNT/${git_count}/g" \
        -e "s/_GIT_HASH/${git_hash}/g" \
        "${spec_file}.in" > "${spec_file}"
    rpmbuild --target=noarch --define "_topdir ${PWD}/redhat/rpmbuild" -bb "${spec_file}"
elif [ ${platform} == "binary" ]; then
    ${mvn_package}
    rm -f ./target/*-tests.jar ./target/*-sources.jar

    # For releases only
    # Expects the ${release} to be defined in the env, i.e. through Jenkins
    if [ -z "$release" ]; then
        echo "No release number defined. Define the \$release environmental variable."; exit 1
    fi
    
    BINARY_NAME="fdb-sql-layer-${release}"
    BINARY_TAR_NAME="${BINARY_NAME}.tar.gz"
    
    mkdir -p ${BINARY_NAME}
    mkdir -p ${BINARY_NAME}/lib/server
    mkdir -p ${BINARY_NAME}/lib/client
    mkdir -p ${BINARY_NAME}/bin
    cp ./target/fdb-sql-layerr-*.jar ${BINARY_NAME}/lib
    cp ./target/dependency/* ${BINARY_NAME}/lib/server/
    cp -R ./conf ${BINARY_NAME}/
    rm -f ${BINARY_NAME}/conf/*.cmd
    cp ./bin/fdbsqllayer ${BINARY_NAME}/bin
    cp packages-common/fdbsql* ${BINARY_NAME}/bin
    cp packages-common/fdb-sql-layer-client-*.jar ${BINARY_NAME}/lib
    cp packages-common/client/* ${BINARY_NAME}/lib/client
    cp LICENSE.txt ${BINARY_NAME}/LICENSE.txt
    tar zcf ${BINARY_TAR_NAME} ${BINARY_NAME}    
elif [ ${platform} == "macosx" ]; then
    ${mvn_package}
    rm -f ./target/*-tests.jar ./target/*-sources.jar

    PKG_DIR=target/packaging/osx
    PKG_ROOT=${PKG_DIR}/root
    PKG_ETC=${PKG_ROOT}/usr/local/etc/foundationdb/sql
    PKG_LOCAL=${PKG_ROOT}/usr/local/foundationdb

    mkdir -p ${PKG_DIR}/
    mkdir -p ${PKG_ROOT}/Library/LaunchDaemons
    mkdir -p ${PKG_ROOT}/usr/local/bin
    mkdir -p ${PKG_ROOT}/usr/local/libexec
    mkdir -p ${PKG_ETC}
    mkdir -p ${PKG_LOCAL}/logs/sql
    mkdir -p ${PKG_LOCAL}/sql/client
    mkdir -p ${PKG_LOCAL}/sql/plugins
    mkdir -p ${PKG_LOCAL}/sql/server

    cp -r macosx/resources ${PKG_DIR}
    cp LICENSE.txt ${PKG_DIR}/resources
    
    install -m 0644 macosx/com.foundationdb.layer.sql.plist ${PKG_ROOT}/Library/LaunchDaemons

    install -m 0644 config-files/* ${PKG_ETC}
    install -m 0644 macosx/config-files/* ${PKG_ETC}

    install -m 0644 LICENSE.txt ${PKG_LOCAL}/LICENSE-SQL_LAYER
    install -m 0755 macosx/uninstall-FoundationDB-SQL_Layer.sh ${PKG_LOCAL}
    install -m 0644 target/fdb-sql-layer-*.jar ${PKG_LOCAL}/sql
    install -m 0644 target/dependency/* ${PKG_LOCAL}/sql/server
    install -m 0644 packages-common/fdb-sql-layer-client-tools-*.jar ${PKG_LOCAL}/sql
    install -m 0644 packages-common/client/* ${PKG_LOCAL}/sql/client
    #install -m 0644 packages-common/plugins/* ${PKG_LOCAL}/sql/plugins

    ln -s /usr/local/foundationdb/sql/fdb-sql-layer-2.0.0-SNAPSHOT.jar ${PKG_LOCAL}/sql/fdb-sql-layer.jar
    ln -s /usr/local/foundationdb/sql/fdb-sql-layer-client-tools-1.3.7-SNAPSHOT.jar ${PKG_LOCAL}/sql/fdb-sql-layer-client-tools.jar

    sed 's/usr\/share/usr\/local/g' bin/fdbsqllayer > packages-common/fdbsqllayer
    install -m 0755 packages-common/fdbsqldump ${PKG_ROOT}/usr/local/bin
    install -m 0755 packages-common/fdbsqlload ${PKG_ROOT}/usr/local/bin
    install -m 0755 packages-common/fdbsqllayer ${PKG_ROOT}/usr/local/libexec

    pkgbuild --root ${PKG_ROOT} --identifier com.foundationdb.layer.sql --version ${layer_version} --scripts macosx/scripts target/SQL_Layer.pkg
    productbuild --distribution macosx/Distribution.xml --resources ${PKG_DIR}/resources --package-path target/ target/FoundationDB-SQL_Layer-${layer_version}.pkg
else
    echo "Invalid Argument: ${platform}"
    echo "${usage}"
    exit 1
fi
