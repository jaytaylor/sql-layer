#!/bin/bash
#
# END USER LICENSE AGREEMENT (“EULA”)
#
# READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
# http://www.akiban.com/licensing/20110913
#
# BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
# ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
# AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
#
# IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
# THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
# NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
# YOUR INITIAL PURCHASE.
#
# IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
# CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
# FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
# LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
# BY SUCH AUTHORIZED PERSONNEL.
#
# IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
# USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
# PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
#

set -e

usage="Usage: ./build_packages.sh [debian|redhat|macosx|binary] [... epoch]"
if [ $# -lt 1 ]; then
    echo "${usage}"
    exit 1
fi

platform=$1
bzr_revno=`bzr revno`
server_version=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep -o '^[0-9.]\+')

echo "Building Akiban Server"

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
# Add akiban-client tools
#
: ${TOOLS_BRANCH:="lp:akiban-client-tools"}
echo "Using akiban-client-tools bazaar branch: ${TOOLS_BRANCH}"
pushd .
cd target
rm -rf akiban-client-tools
bzr branch ${TOOLS_BRANCH} akiban-client-tools
cd akiban-client-tools
mvn -DskipTests=true install 
rm -f target/*-tests.jar target/*-sources.jar
cp bin/akdump ../../packages-common/
cp target/akiban-client-tools-*.jar ../../packages-common/
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
    mvn -Dmaven.test.skip.exec clean install -DBZR_REVISION=${bzr_revno}
    mkdir -p ${platform}/server/
    cp ./target/dependency/* ${platform}/server/
    cp -R packages-common/plugins/ ${platform}/
    debuild
elif [ ${platform} == "redhat" ]; then
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    tar_file=${PWD}/redhat/rpmbuild/SOURCES/akserver.tar
    bzr export --format=tar $tar_file
    rm -f ${PWD}/redhat/akserver/redhat/* # Clear out old files
    cp -R packages-common/* ${PWD}/redhat/akserver/redhat
    pushd redhat
    # bzr st -S outs lines like "? redhat/akserver/redhat/log4j.properties"
    # we want to turn those to just "akserver/redhat/log4j.properties"
    for to_add in $(bzr st -S . | sed 's/\?\s\+redhat\///'); do
        tar --append -f $tar_file $to_add
    done
    popd
    gzip $tar_file
    cat ${PWD}/redhat/akiban-server.spec | sed "9,9s/REVISION/${bzr_revno}/g" > ${PWD}/redhat/akiban-server-${bzr_revno}.spec
    sed -i "10,10s/EPOCH/${epoch}/g" ${PWD}/redhat/akiban-server-${bzr_revno}.spec
    rpmbuild --target=noarch --define "_topdir ${PWD}/redhat/rpmbuild" -ba ${PWD}/redhat/akiban-server-${bzr_revno}.spec
elif [ ${platform} == "binary" ]; then
    mvn -Dmaven.test.skip clean install -DBZR_REVISION=${bzr_revno}
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
    client_jar=packages-common/akiban-client-tools-*.jar
    client_deps=packages-common/client
    akdump_bin=packages-common/akdump
    plugins_dir=packages-common/plugins
    mac_app='target/Akiban Server.app'
    inst_temp=/tmp/inst_temp

    # copy icon data from a "prototype" file
    tar xzf macosx/license-icon.tar.gz
    xattr -wx com.apple.FinderInfo "`xattr -px com.apple.FinderInfo prototype.txt`" ${license}
    cp prototype.txt/..namedfork/rsrc ${license}/..namedfork/rsrc
    rm prototype.txt
    
    # build jar
    mvn -DskipTests=true -DBZR_REVISION=${bzr_revno} clean install 
    rm -f ./target/*-tests.jar ./target/*-sources.jar

    build_dmg() {
        ant_target="$1"
        mac_dmg="target/$2"

        # build app bundle
        curl -Ls -o target/appbundler-1.0.jar http://java.net/projects/appbundler/downloads/download/appbundler-1.0.jar
        ant -f macosx/appbundler.xml ${ant_target} -Djdk.home=$(/usr/libexec/java_home) -Dakserver.version="${server_version}-r${bzr_revno}"

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
        mkdir "$inst_temp/Akiban Server.app"
        ln -s /Applications $inst_temp
        mkdir $inst_temp/.background
        cp macosx/dmg_background.png $inst_temp/.background
        hdiutil create -fs HFSX -layout SPUD -size 200m $inst_temp.dmg -format UDRW -volname 'Akiban Server' -srcfolder $inst_temp
        rm -rf $inst_temp

        # update disk image
        mkdir $inst_temp
        hdiutil attach $inst_temp.dmg -noautoopen -mountpoint $inst_temp
        ditto "$mac_app" "$inst_temp/Akiban Server.app"
        
        # == add non-app files here ==
        cp macosx/dmg.DS_Store $inst_temp/.DS_Store
        cp macosx/dmg_VolumeIcon.icns $inst_temp/.VolumeIcon.icns
        cp ${license} $inst_temp/LICENSE.txt
        SetFile -a C $inst_temp
        hdiutil detach `hdiutil info | grep $inst_temp | grep '^/dev' | cut -f1`
        hdiutil convert $inst_temp.dmg -format UDZO -imagekey zlib-level=9 -o "$mac_dmg"
        rm $inst_temp.dmg
    }
    
    dmg_basename="Akiban_Server_${server_version}"
    build_dmg "bundle_app" "${dmg_basename}.dmg"
    build_dmg "bundle_app_jre" "${dmg_basename}_JRE.dmg"
else
    echo "Invalid Argument: ${platform}"
    echo "${usage}"
    exit 1
fi
