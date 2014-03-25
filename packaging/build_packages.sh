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

usage() {
    echo "Usage: $0 [-e EPOCH] [-h] [-r] <deb|rpm|pkg|targz>" >&2
    echo "    -e  Epoch number (default is current timestamp, ignored for release)" >&2
    echo "    -h  Display this help message" >&2
    echo "    -r  Building a release" >&2
}

ARGS=$(getopt "e:hr" "${@}")
if [ "$?" != "0" ]; then
    usage
    exit 1
fi

RELEASE="0"
eval set -- "${ARGS}"
while true; do
    case "${1}" in
        -e)
            EPOCH="${2}"
            shift 2
        ;;
        -h)
            usage
            exit 0
        ;;
        -r)
            RELEASE="1"
            shift 1
        ;;
        --)
            shift
            break
        ;;
    esac
done

if [ "$2" != "" ]; then
    echo "Unexpected extra arguments" >&2
    usage
    exit 1
fi


set -e

PACKAGING_DIR=$(cd $(dirname "${0}") ; echo "${PWD}")
TOP_DIR=$(cd "${PACKAGING_DIR}/.." ; echo "${PWD}")
STAGE_DIR="${TOP_DIR}/target/packaging"


mvn_package() {
    mvn clean package -q -B -U -DskipTests=true -Dfdbsql.release=${RELEASE} >/dev/null
}

# $1 - output bin dir
# $2 - output jar dir
build_sql_layer() {
    if [ "$1" = "" -o "$2" = "" ]; then
        echo "Missing argument" >&2
        exit 1
    fi

    LAYER_MVN_VERSION=$(cd "${TOP_DIR}" ; mvn -o org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep '^[0-9]')
    LAYER_VERSION=${LAYER_MVN_VERSION%-SNAPSHOT}
    LAYER_JAR_NAME="fdb-sql-layer-${LAYER_MVN_VERSION}.jar"
    
    echo "Building FoundationDB SQL Layer ${LAYER_VERSION} Release ${RELEASE}"
    pushd .
    cd "${TOP_DIR}"
    mvn_package
    mkdir -p "${1}" "${2}"/{server,plugins}
    cp "${TOP_DIR}/bin/fdbsqllayer" "${1}/"
    cp "target/${LAYER_JAR_NAME}" "${2}/"
    cp target/dependency/* "${2}/server/"
    popd
}

# $1 - output bin dir
# $2 - output jar dir
build_client_tools() {
    : ${TOOLS_LOC:="git@github.com:FoundationDB/sql-layer-client-tools.git"}
    : ${TOOLS_REF:="master"}
    CLIENT_JAR_NAME=""
    
    echo "Building client-tools: ${TOOLS_REF} on ${TOOLS_LOC}"
    pushd .
    cd "${TOP_DIR}/target"
    rm -rf client-tools
    git clone -q "${TOOLS_LOC}" client-tools
    cd client-tools
    git checkout -b scratch "${TOOLS_REF}"
    mvn_package
    cd target/
    for f in $(ls fdb-sql-layer-client-tools*.jar |grep -v -E 'source|test'); do
        CLIENT_JAR_NAME="$f"
    done
    cd ..
    if [ -z "${CLIENT_JAR_NAME}" ]; then
        echo "No client jar found" >&2
        exit 1
    fi
    mkdir -p "${1}" "${2}/client"
    cd bin
    for f in $(ls fdbsql* |grep -v '\.cmd'); do
        cp "${f}" "${1}/"
    done
    cd ..
    cp target/${CLIENT_JAR_NAME} "${2}/"
    cp target/dependency/* "${2}/client/"
    popd
}

# $1 = output conf dir
# $2 = install data dir
# $3 = install log dir
# $4 = install temp dir
filter_config_files() {
    if [ "$1" = "" -o "$2" = "" -o "$3" = "" -o "$4" = "" ]; then
        echo "Missing argument" >&2
        exit 1
    fi
    mkdir -p "${1}"
    pushd .
    cd "${PACKAGING_DIR}/conf"
    for f in $(ls); do
        sed -e "s|\\\${datadir}|${2}|g" \
            -e "s|\\\${logdir}|${3}|g" \
            -e "s|\\\${tempdir}|${4}|g" \
            "${f}" > "${1}/${f}"
    done
    popd
}

case "${1}" in
    "deb")
        build_sql_layer "${STAGE_DIR}/usr/sbin"  "${STAGE_DIR}/usr/share/foundationdb/sql"
        filter_config_files "${STAGE_DIR}/etc/foundationdb/sql" "/var/lib/foundationdb/sql" "/var/log/foundationdb/sql" "/tmp"

        cp -r "${PACKAGING_DIR}/deb" "${STAGE_DIR}/debian"
        mkdir -p "${STAGE_DIR}/usr/share/doc/fdb-sql-layer/"
        cp "${PACKAGING_DIR}/deb/copyright" "${STAGE_DIR}/usr/share/doc/fdb-sql-layer/"

        cd "${STAGE_DIR}/debian"
        sed -e "s/VERSION/${LAYER_VERSION}/g" -e "s/RELEASE/${RELEASE}/g" changelog.in > changelog
        sed -e "s/LAYER_JAR_NAME/${LAYER_JAR_NAME}/g" links.in > links
        cd ..

        # No sign source, no sign changes, binary only
        debuild -us -uc -b
    ;;

    "rpm")
        if [ -z "${EPOCH}" ]; then
            EPOCH=$(date +%s)
        fi
        # Releases shouldn't have epochs
        if [ ${RELEASE} -gt 0 ]; then
            EPOCH="0"
        fi

        STAGE_ROOT="${STAGE_DIR}/rpmbuild"
        BUILD_DIR="${STAGE_ROOT}/BUILD"
        build_sql_layer "${BUILD_DIR}/usr/sbin" "${BUILD_DIR}/usr/share/foundationdb/sql"
        filter_config_files "${BUILD_DIR}/etc/foundationdb/sql" "/var/lib/foundationdb/sql" "/var/log/foundationdb/sql" "/tmp"

        mkdir -p "${BUILD_DIR}/etc/rc.d/init.d/"
        cp "${PACKAGING_DIR}/rpm/fdb-sql-layer.init" "${BUILD_DIR}/etc/rc.d/init.d/fdb-sql-layer"
        
        mkdir -p "${BUILD_DIR}/usr/share/doc/fdb-sql-layer/"
        cp "${TOP_DIR}/LICENSE.txt" ${BUILD_DIR}/usr/share/doc/fdb-sql-layer/LICENSE

        if [ "${EPOCH}" != "0" ]; then
            echo "Epoch: ${EPOCH}"
        fi

        mkdir -p "${STAGE_ROOT}"/{SOURCES,SRPMS,RPMS/noarch}
        rpmbuild --target=noarch -bb "${PACKAGING_DIR}/rpm/fdb-sql-layer.spec" \
            --define "_topdir ${STAGE_ROOT}" \
            --define "_fdb_sql_version ${LAYER_VERSION}" \
            --define "_fdb_sql_release ${RELEASE}" \
            --define "_fdb_sql_layer_jar ${LAYER_JAR_NAME}" \
            --define "_fdb_sql_epoch ${EPOCH}"

        mv "${STAGE_ROOT}"/RPMS/noarch/* "${TOP_DIR}/target/"
    ;;

    "targz")
        STAGE_ROOT="${STAGE_DIR}/targz"

        cd "${TOP_DIR}"
        build_sql_layer "${STAGE_ROOT}/bin" "${STAGE_ROOT}/lib"
        filter_config_files "${STAGE_ROOT}/conf" "./data" "./logs" "./tmp"

        cp bin/* "${STAGE_ROOT}/bin/"
        cp LICENSE.txt "${STAGE_ROOT}/"
        cp README.md "${STAGE_ROOT}/"

        BINARY_NAME="fdb-sql-layer-${LAYER_VERSION}-${RELEASE}"
        cd "${STAGE_DIR}"
        mv targz "${BINARY_NAME}"
        tar czf "${TOP_DIR}/target/${BINARY_NAME}.tar.gz" "${BINARY_NAME}"
    ;;

    "pkg")
        STAGE_ROOT="${STAGE_DIR}/root"
        STAGE_LOCAL="${STAGE_ROOT}/usr/local"

        build_sql_layer "${STAGE_LOCAL}/libexec" "${STAGE_LOCAL}/foundationdb/sql"
        build_client_tools "${STAGE_LOCAL}/bin" "${STAGE_LOCAL}/foundationdb/sql"
        filter_config_files "${STAGE_LOCAL}/etc/foundationdb/sql" "/usr/local/foundationdb/data/sql" "/usr/local/foundationdb/logs/sql" "/tmp"

        mkdir -p "${STAGE_ROOT}/Library/LaunchDaemons"
        mkdir -p "${STAGE_LOCAL}/foundationdb/logs/sql"

        cp "${TOP_DIR}/LICENSE.txt" "${STAGE_LOCAL}/foundationdb/LICENSE-SQL_LAYER"
        
        cd "${PACKAGING_DIR}/pkg/"
        cp -r resources/ "${STAGE_DIR}/"
        cp "${TOP_DIR}/LICENSE.txt" "${STAGE_DIR}/resources/"
        cp com.foundationdb.layer.sql.plist "${STAGE_ROOT}/Library/LaunchDaemons/"
        cp uninstall-FoundationDB-SQL_Layer.sh "${STAGE_LOCAL}/foundationdb"

        cd "${STAGE_LOCAL}/foundationdb/sql/"
        ln -s /usr/local/foundationdb/sql/${LAYER_JAR_NAME} fdb-sql-layer.jar
        ln -s /usr/local/foundationdb/sql/${CLIENT_JAR_NAME} fdb-sql-layer-client-tools.jar

        cd "${TOP_DIR}/target"
        pkgbuild --root "${STAGE_ROOT}" --identifier com.foundationdb.layer.sql --version ${LAYER_VERSION}.${RELEASE} --scripts "${PACKAGING_DIR}/pkg/scripts" "${STAGE_DIR}/SQL_Layer.pkg"
        productbuild --distribution "${PACKAGING_DIR}/pkg/Distribution.xml" --resources "${STAGE_DIR}/resources" --package-path "${STAGE_DIR}" fdb-sql-layer-${LAYER_VERSION}-${RELEASE}.pkg
    ;;

    *)
        usage
        if [ "${1}" = "" ]; then
            echo "Package type required" >&2
        else
            echo "Unknown package type: ${1}" >&2
        fi
        exit 1
    ;;
esac

exit 0

