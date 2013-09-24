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

PACKAGING_DIR=$(cd $(dirname "${0}") ; echo "${PWD}")
TOP_DIR=$(cd "${PACKAGING_DIR}/.." ; echo "${PWD}")
STAGE_DIR="${TOP_DIR}/target/packaging"


GIT_HASH=`git rev-parse --short HEAD`
GIT_COUNT=`git rev-list --merges HEAD |wc -l |tr -d ' '` # --count is newer

mvn_package() {
    mvn clean package -q -B -U -DGIT_COUNT=${GIT_COUNT} -DGIT_HASH=${GIT_HASH} -DskipTests=true
}

# $1 - output bin/ dir
# $2 - output conf/ dir
# $3 - output lib/ dir
init_common() {
    if [ "$1" = "" -o "$2" = "" -o "$3" = "" ]; then
        echo "Missing argument" >&2
        exit 1
    fi

    LAYER_MVN_VERSION=$(cd "${TOP_DIR}" ; mvn -o org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep '^[0-9]')
    LAYER_VERSION=${LAYER_MVN_VERSION%-SNAPSHOT}
    
    echo "Building FoundationDB SQL Layer ${LAYER_VERSION}"
    pushd .
    cd "${TOP_DIR}"
    mvn_package
    mkdir -p "${1}" "${2}" "${3}/server"
    cp "${TOP_DIR}/bin/fdbsqllayer" "${1}/"
    cp "${PACKAGING_DIR}"/conf/* "${2}/"
    cp target/fdb-sql-layer-${LAYER_MVN_VERSION}.jar "${3}/"
    cp target/dependency/* "${3}/server/"
    popd
}

# $1 - output bin/ dir
# $2 - output lib/ dir
build_client_tools() {
    : ${TOOLS_LOC:="git@github.com:FoundationDB/sql-layer-client-tools.git"}
    
    echo "Building client-tools: ${TOOLS_LOC}"
    pushd .
    cd "${TOP_DIR}/target"
    rm -rf client-tools
    git clone -q "${TOOLS_LOC}" client-tools
    cd client-tools
    mvn clean package -q -B -U -DskipTests=true
    rm -f target/*-tests.jar target/*-sources.jar
    mkdir -p "${1}" "${2}/client"
    cp bin/fdbsql{dump,load} "${1}/"
    cp target/fdb-sql-layer-client-tools-*.jar "${2}/"
    cp target/dependency/* "${2}/client/"
    popd
}

case "$1" in
    "--git-hash")
        echo "${GIT_HASH}"
    ;;
    
    "--git-count")
        echo "${GIT_COUNT}"
    ;;

    "deb")
        cp -R packages-common/* ${platform}
        ${mvn_package}
        mkdir -p ${platform}/server/
        cp ./target/dependency/* ${platform}/server/
        cp -R packages-common/plugins/ ${platform}/
        # No sign source, no sign changes, binary only
        debuild -us -uc -b
    ;;

    "rpm")
        if [ -z "$2" ] ; then
            epoch=$(date +%s)
        else
            epoch=$2
        fi

        rm -rf ${PWD}/redhat/rpmbuild
        mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
        BUILD="${PWD}/redhat/rpmbuild/BUILD"
        ${mvn_package}
        cp -r packages-common/* target/ redhat/fdb-sql-layer.init "${BUILD}"
        spec_file="$PWD/redhat/fdb-sql-layer.spec"
        sed -e "s/_EPOCH/${epoch}/g" \
            -e "s/_GIT_COUNT/${GIT_COUNT}/g" \
            -e "s/_GIT_HASH/${GIT_HASH}/g" \
            "${spec_file}.in" > "${spec_file}"

        rpmbuild --target=noarch --define "_topdir ${PWD}/redhat/rpmbuild" -bb "${spec_file}"
    ;;

    "targz")
        ${mvn_package}
        rm -f ./target/*-tests.jar ./target/*-sources.jar

        # For releases only
        # Expects the ${release} to be defined in the env, i.e. through Jenkins
        if [ -z "$release" ]; then
            echo 'No release number defined. Define the $release environmental variable.'
            exit 1
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
    ;;

    "pkg")
        STAGE_ROOT="${STAGE_DIR}/root"
        STAGE_LOCAL="${STAGE_ROOT}/usr/local"

        init_common "${STAGE_LOCAL}/libexec" "${STAGE_LOCAL}/etc/foundationdb/sql" "${STAGE_LOCAL}/foundationdb/sql"
        build_client_tools "${STAGE_LOCAL}/bin" "${STAGE_LOCAL}/foundationdb/sql"

        mkdir -p "${STAGE_ROOT}/Library/LaunchDaemons"
        mkdir -p "${STAGE_LOCAL}/foundationdb/logs/sql"

        sed -i "" -e 's/usr\/share/usr\/local/g' "${STAGE_LOCAL}/libexec/fdbsqllayer"
        cp "${TOP_DIR}/LICENSE.txt" "${STAGE_LOCAL}/foundationdb/LICENSE-SQL_LAYER"
        
        cd "${PACKAGING_DIR}/pkg/"
        cp -r resources/ "${STAGE_DIR}/"
        cp "${TOP_DIR}/LICENSE.txt" "${STAGE_DIR}/resources/"
        cp com.foundationdb.layer.sql.plist "${STAGE_ROOT}/Library/LaunchDaemons/"
        cp conf/* "${STAGE_LOCAL}/etc/foundationdb/sql/"
        cp uninstall-FoundationDB-SQL_Layer.sh "${STAGE_LOCAL}/foundationdb"

        cd "${STAGE_LOCAL}/foundationdb/sql/"
        ln -s /usr/local/foundationdb/sql/fdb-sql-layer-2.0.0-SNAPSHOT.jar fdb-sql-layer.jar
        ln -s /usr/local/foundationdb/sql/fdb-sql-layer-client-tools-1.3.7-SNAPSHOT.jar fdb-sql-layer-client-tools.jar

        cd "${TOP_DIR}/target"
        pkgbuild --root "${STAGE_ROOT}" --identifier com.foundationdb.layer.sql --version ${LAYER_VERSION} --scripts "${PACKAGING_DIR}/pkg/scripts" "${STAGE_DIR}/SQL_Layer.pkg"
        productbuild --distribution "${PACKAGING_DIR}/pkg/Distribution.xml" --resources "${STAGE_DIR}/resources" --package-path "${STAGE_DIR}" FoundationDB-SQL_Layer-${LAYER_VERSION}.pkg
    ;;

    *)
        echo "Usage: $0 [--git-hash | --git-count | deb | rpm [epoc] | pkg | targz]" >&2
        if [ "$1" != "" ]; then
            echo "Invalid Argument: $1" >&2
        fi
        exit 1
    ;;
esac

exit 0

