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

    LAYER_MVN_VERSION=$(cd "${TOP_DIR}/fdb-sql-layer-core" ; mvn -B org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep '^[0-9]')
    LAYER_VERSION=${LAYER_MVN_VERSION%-SNAPSHOT}
    LAYER_JAR_NAME="fdb-sql-layer-core-${LAYER_MVN_VERSION}.jar"

    RF_MVN_VERSION=$(cd "${TOP_DIR}/routine-firewall" ; mvn -B org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep '^[0-9]')
    RF_VERSION=${RF_MVN_VERSION%-SNAPSHOT}
    RF_JAR_NAME="routine-firewall-${RF_MVN_VERSION}.jar"    


    echo "Building FoundationDB SQL Layer ${LAYER_VERSION} Release ${RELEASE}"
    pushd .
    cd "${TOP_DIR}"
    mvn_package
    mkdir -p "${1}" "${2}"/{server,plugins,routine-firewall}
    cp "${TOP_DIR}/bin/fdbsqllayer" "${1}/"
    cp "fdb-sql-layer-core/target/${LAYER_JAR_NAME}" "${2}/"
    cp fdb-sql-layer-core/target/dependency/* "${2}/server/"
    cp "routine-firewall/target/${RF_JAR_NAME}" "${2}/routine-firewall/"
    cp "sql-layer.policy" "${2}/"
    popd
}

# $1 - output bin dir
# $2 - output jar dir
build_client_tools() {
    : ${TOOLS_LOC:="git@github.com:FoundationDB/sql-layer-client-tools.git"}
    : ${TOOLS_REF:="master"}
    
    pushd .
    mkdir -p "${1}"
    mkdir -p "${2}"
    cd "${TOP_DIR}/target/packaging"
    rm -rf client-tools
    git clone -q "${TOOLS_LOC}" client-tools
    cd client-tools
    git checkout -b scratch "${TOOLS_REF}"
    if [ "${RELEASE}" = "1" ]; then
        CLIENT_RELEASE="-r"
    else
        CLIENT_RELEASE=""
    fi
    bash packaging/build_packages.sh "${CLIENT_RELEASE}" targz
    cd target/packaging/targz/
    DIR=$(ls)
    cp "${DIR}"/bin/* "${1}"
    cp -r "${DIR}"/lib/* "${2}"
    cd "${DIR}/lib"
    CLIENT_JAR_NAME=$(ls *.jar)
    popd
}

# $1 = output conf dir
# $2 = install data dir
# $3 = install log dir
# $4 = install temp dir
# $5 = extension (including .) for filtered file (optional)
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
            "${f}" > "${1}/${f}${5}"
    done
    popd
}

case "${1}" in
    "deb")
        umask 0022
        build_sql_layer "${STAGE_DIR}/usr/sbin"  "${STAGE_DIR}/usr/share/foundationdb/sql"

        cd "${STAGE_DIR}"
        mkdir -p -m 0755 DEBIAN
        mkdir -p -m 0755 etc/foundationdb/sql
        mkdir -p -m 0755 etc/init.d
        mkdir -p -m 0755 usr/share/doc/fdb-sql-layer
        mkdir -p -m 0755 usr/share/foundationdb/sql/{plugins,server,routine-firewall}
        mkdir -p -m 0755 var/{lib,log}/foundationdb/sql

        install -m 0644 "${TOP_DIR}/packaging/deb/conffiles" "${STAGE_DIR}/DEBIAN/"
        install -m 0755 "${TOP_DIR}/packaging/deb/"{pre,post}* "${STAGE_DIR}/DEBIAN/"
        install -m 0755 "${TOP_DIR}/packaging/deb/fdb-sql-layer.init" "${STAGE_DIR}/etc/init.d/fdb-sql-layer"

        filter_config_files "${STAGE_DIR}/etc/foundationdb/sql" \
              "/var/lib/foundationdb/sql" "/var/log/foundationdb/sql" "/tmp"
        install -m 0644 "${PACKAGING_DIR}/deb/copyright" "usr/share/doc/fdb-sql-layer/"

        sed -e "s/VERSION/${LAYER_VERSION}/g" -e "s/RELEASE/${RELEASE}/g" \
              "${PACKAGING_DIR}/deb/fdb-sql-layer.control.in"  > DEBIAN/control

        cd usr/share/foundationdb/sql
        ln -s "${LAYER_JAR_NAME}" "fdb-sql-layer.jar"
        cd routine-firewall/
        ln -s "${RF_JAR_NAME}" "routine-firewall.jar"     

        cd "${STAGE_DIR}"
        echo "Installed-Size:" $(du -sx --exclude DEBIAN $STAGE_DIR | awk '{print $1}') >> "${STAGE_DIR}/DEBIAN/control"
        
        fakeroot dpkg-deb --build . "${TOP_DIR}/target"
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
        #
        # SQL Layer
        #
        LAYER_ROOT="${STAGE_DIR}/sql_layer_root"
        LAYER_ULOCAL="${LAYER_ROOT}/usr/local"
        build_sql_layer "${LAYER_ULOCAL}/libexec" "${LAYER_ULOCAL}/foundationdb/sql"
        filter_config_files "${LAYER_ULOCAL}/etc/foundationdb/sql" "/usr/local/foundationdb/data/sql" "/usr/local/foundationdb/logs/sql" "/tmp" ".new"

        cd "${STAGE_DIR}"
        mkdir -p "${LAYER_ROOT}/Library/LaunchDaemons"
        mkdir -p "${LAYER_ULOCAL}/foundationdb/logs/sql"
        cd "${PACKAGING_DIR}/pkg"
        cp -r resources "${STAGE_DIR}/"
        cp com.foundationdb.layer.sql.plist "${LAYER_ROOT}/Library/LaunchDaemons/"
        cp uninstall-FoundationDB-SQL_Layer.sh "${LAYER_ULOCAL}/foundationdb"
        cp "${TOP_DIR}/LICENSE.txt" "${LAYER_ULOCAL}/foundationdb/LICENSE-SQL_LAYER"
        cp "${TOP_DIR}/LICENSE.txt" "${STAGE_DIR}/resources/"
        cd "${LAYER_ULOCAL}/foundationdb/sql/"
        ln -s /usr/local/foundationdb/sql/${LAYER_JAR_NAME} fdb-sql-layer.jar
        cd routine-firewall
        ln -s /usr/local/foundationdb/sql/routine-firewall/${RF_JAR_NAME} routine-firewall.jar
        #
        # Client Tools
        #
        cd "${STAGE_DIR}"
        CLIENT_ROOT="${STAGE_DIR}/client_tools_root"
        CLIENT_ULOCAL="${CLIENT_ROOT}/usr/local"
        build_client_tools "${CLIENT_ULOCAL}/bin" "${CLIENT_ULOCAL}/foundationdb/sql"
        cp "${TOP_DIR}/target/packaging/client-tools/LICENSE.txt" "${CLIENT_ULOCAL}/foundationdb/LICENSE-SQL_LAYER_CLIENT_TOOLS"
        cp "${PACKAGING_DIR}/pkg/uninstall-FoundationDB-SQL_Layer.sh" "${CLIENT_ULOCAL}/foundationdb"
        cd "${CLIENT_ULOCAL}/foundationdb/sql/"
        ln -s /usr/local/foundationdb/sql/${CLIENT_JAR_NAME} fdb-sql-layer-client-tools.jar
        #
        # pkgs
        #
        VER_REL="${LAYER_VERSION}.${RELEASE}"
        pkgbuild --root "${LAYER_ROOT}" --identifier com.foundationdb.layer.sql --version ${VER_REL} --scripts "${PACKAGING_DIR}/pkg/scripts" "${STAGE_DIR}/SQL_Layer.pkg"
        pkgbuild --root "${CLIENT_ROOT}" --identifier com.foundationdb.layer.sql.client.tools --version ${VER_REL} "${STAGE_DIR}/SQL_Layer_Client_Tools.pkg"
        productbuild --distribution "${PACKAGING_DIR}/pkg/Distribution.xml" --resources "${STAGE_DIR}/resources" --package-path "${STAGE_DIR}" "${TOP_DIR}/target/fdb-sql-layer-${LAYER_VERSION}-${RELEASE}.pkg"
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

