#!/bin/bash
#
# Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

usage="Usage: ./build_packages.sh [debian|redhat|macosx|binary] [... epoch]"
if [ $# -lt 1 ]; then
    echo "${usage}"
    exit 1
fi

platform=$1
git_hash=`git rev-parse --short HEAD`
git_count=`git rev-list --merges HEAD |wc -l |tr -d ' '` # --count is newer
layer_version=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep -o '^[0-9.]\+')

mvn_install="mvn clean install -DGIT_COUNT=${git_count} -DGIT_HASH=${git_hash} -DskipTests=true"

echo "Building FoundationDB SQL Layer"

license=LICENSE.txt

mkdir -p target
mkdir -p packages-common/client
mkdir -p packages-common/plugins
common_dir="config-files/"
if [ ! -d ${common_dir} ]; then
    echo "fatal: Couldn't find configuration files in: ${common_dir}"
    exit 1
fi
echo "-- packages-common directory: ${common_dir} (Linux only)"
# All licenses become LICENSE.txt
cp ${license} packages-common/LICENSE.txt
cp ${common_dir}/* packages-common/

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
mvn -DskipTests=true install 
rm -f target/*-tests.jar target/*-sources.jar
cp bin/fdbsqldump ../../packages-common/
cp target/foundationdb-sql-layer-client-tools-*.jar ../../packages-common/
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
    $mvn_install
    mkdir -p ${platform}/server/
    cp ./target/dependency/* ${platform}/server/
    cp -R packages-common/plugins/ ${platform}/
    debuild
elif [ ${platform} == "redhat" ]; then
    rm -rf ${PWD}/redhat/{fdbsql,rpmbuild}
    mkdir -p ${PWD}/redhat/fdbsql/redhat
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    tar_prefix=fdbsql
    tar_file=${PWD}/redhat/rpmbuild/SOURCES/${tar_prefix}.tar
    git archive --format=tar --output="$tar_file" --prefix="${tar_prefix}/" HEAD
    rm -f ${PWD}/redhat/fdbsql/redhat/* # Clear out old files
    cp -R packages-common/* ${PWD}/redhat/fdbsql/redhat
    pushd redhat
    # git status outputs lines like "?? redhat/fdbsql/"
    # we want to turn those to just "fdbsql/"
    for to_add in $(git status --untracked=normal --porcelain | sed 's/\?\+\s\+redhat\///'); do
        tar --append -f $tar_file $to_add
    done
    popd
    gzip $tar_file
    cat ${PWD}/redhat/fdb-sql-layer.spec | \
        sed -e "10,10s/_EPOCH/${epoch}/g" \
            -e "s/_GIT_COUNT/${git_count}/g" -e "s/_GIT_HASH/${git_hash}/g" \
        > ${PWD}/redhat/akiban-server-${git_count}.spec
    rpmbuild --target=noarch --define "_topdir ${PWD}/redhat/rpmbuild" -ba ${PWD}/redhat/akiban-server-${git_count}.spec
elif [ ${platform} == "binary" ]; then
    $mvn_install
    rm -f ./target/*-tests.jar ./target/*-sources.jar

    # For releases only
    # Expects the ${release} to be defined in the env, i.e. through Jenkins
    if [ -z "$release" ]; then
        echo "No release number defined. Define the \$release environmental variable."; exit 1
    fi
    
    BINARY_NAME="akiban-server-${release}"
    BINARY_TAR_NAME="${BINARY_NAME}.tar.gz"
    
    mkdir -p ${BINARY_NAME}
    mkdir -p ${BINARY_NAME}/lib/server
    mkdir -p ${BINARY_NAME}/lib/client
    mkdir -p ${BINARY_NAME}/bin
    cp ./target/akiban-server-*.jar ${BINARY_NAME}/lib
    cp ./target/dependency/* ${BINARY_NAME}/lib/server/
    cp -R ./conf ${BINARY_NAME}/
    rm -f ${BINARY_NAME}/conf/config/*.cmd
    cp ./bin/akserver ${BINARY_NAME}/bin
    cp packages-common/akdump ${BINARY_NAME}/bin
    cp packages-common/akiban-client-*.jar ${BINARY_NAME}/lib
    cp packages-common/client/* ${BINARY_NAME}/lib/client
    cp ${license} ${BINARY_NAME}/LICENSE.txt
    tar zcf ${BINARY_TAR_NAME} ${BINARY_NAME}    
elif [ ${platform} == "macosx" ]; then
    client_jar=packages-common/foundationdb-sql-layer-client-tools-*.jar
    client_deps=packages-common/client
    akdump_bin=packages-common/fdbsqldump
    plugins_dir=packages-common/plugins
    app_name='FoundationDB SQL Layer.app'
    mac_app="target/${app_name}"
    inst_temp=/tmp/inst_temp

    # copy icon data from a "prototype" file
    tar xzf macosx/license-icon.tar.gz
    xattr -wx com.apple.FinderInfo "`xattr -px com.apple.FinderInfo prototype.txt`" ${license}
    cp prototype.txt/..namedfork/rsrc ${license}/..namedfork/rsrc
    rm prototype.txt
    
    # build jar
    $mvn_install
    rm -f ./target/*-tests.jar ./target/*-sources.jar

    build_dmg() {
        ant_target="$1"
        mac_dmg="target/$2"

        # build app bundle
        curl -Ls -o target/appbundler-1.0.jar http://java.net/projects/appbundler/downloads/download/appbundler-1.0.jar
        ant -f macosx/appbundler.xml ${ant_target} -Djdk.home=$(/usr/libexec/java_home) -Dfdbsql.version="${layer_version}-r${git_count}"

        # add config files to bundle
        mkdir "${mac_app}/Contents/Resources/config/"
        cp macosx/config-files/* "${mac_app}/Contents/Resources/config/"

        # add client dependencies and binaries to bundle
        mkdir -p "$mac_app/Contents/Resources/tools/lib/client"
        cp $client_jar "$mac_app/Contents/Resources/tools/lib/"
        cp $client_deps/* "$mac_app/Contents/Resources/tools/lib/client/"
        mkdir -p "$mac_app/Contents/Resources/tools/bin"
        cp $akdump_bin "$mac_app/Contents/Resources/tools/bin/"
        cp -R "$plugins_dir" "$mac_app/Contents/Resources/"

        # build disk image template
        rm -rf $inst_temp
        rm -f $inst_temp.dmg
        mkdir $inst_temp
        mkdir "$inst_temp/${app_name}"
        hdiutil create -fs HFSX -layout SPUD -size 200m $inst_temp.dmg -format UDRW -volname 'FoundationDB SQL Layer' -srcfolder $inst_temp
        rm -rf $inst_temp

        # update disk image
        mkdir $inst_temp
        hdiutil attach $inst_temp.dmg -noautoopen -mountpoint $inst_temp
        ditto "$mac_app" "$inst_temp/${app_name}"
        
        # == add non-app files here ==
        ln -s /Applications $inst_temp
        cp macosx/dmg_background.png ${inst_temp}/.background.png
        cp macosx/dmg.DS_Store $inst_temp/.DS_Store
        cp macosx/dmg_VolumeIcon.icns $inst_temp/.VolumeIcon.icns
        cp ${license} $inst_temp/LICENSE.txt
        SetFile -a C $inst_temp
        hdiutil detach `hdiutil info | grep $inst_temp | grep '^/dev' | cut -f1`
        hdiutil convert $inst_temp.dmg -format UDZO -imagekey zlib-level=9 -o "$mac_dmg"
        rm $inst_temp.dmg
    }
    
    dmg_basename="FoundationDB_SQL_Layer_${layer_version}"
    build_dmg "bundle_app" "${dmg_basename}.dmg"
    build_dmg "bundle_app_jre" "${dmg_basename}_JRE.dmg"
else
    echo "Invalid Argument: ${platform}"
    echo "${usage}"
    exit 1
fi
