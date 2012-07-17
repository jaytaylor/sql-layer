#!/bin/bash -x
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

usage="Usage: ./build_packages.sh [debian|redhat|macosx] [... epoch]"
if [ $# -lt 1 ]; then
    echo "${usage}"
    exit 1
fi

platform=$1
bzr_revno=`bzr revno`

# Handle file preparation for release target
if [ -z "${AKIBAN_CE_FLAG}" ]; then
    target='enterprise'
else
    target='community'
fi
echo "Building Akiban Server for ##### ${target} #####"

# Select the correct license. Handled as a special case to keep LICENSE*.txt files in the top level
case "${target}" in
    'enterprise') license=LICENSE-EE.txt;;
    'community')  license=LICENSE-CE.txt;;
    *) echo "fatal: Invalid release type (name: [{$target}]). Check that \
     the script is handling condition flags correctly."
        exit 1
        ;;
esac

mkdir -p target
mkdir -p packages-common
common_dir="config-files/${target}" # require config-files/dir to be the same as the ${target} variable
[ -d ${common_dir} ] || \
    { echo "fatal: Couldn't find configuration files in: ${common_dir}"; exit 1; }
echo "-- packages-common directory: ${common_dir} (Linux only)"
cp ${license} packages-common/LICENSE.txt # All licenses become LICENSE.txt
cp ${common_dir}/* packages-common/

# Add akiban-client tools via `bzr root`/target/akiban-client-tools
tools_branch="lp:akiban-client-tools"
pushd target && bzr branch ${tools_branch} && pushd akiban-client-tools
mvn  -Dmaven.test.skip.exec clean install

# Linux and Mac
cp bin/akdump ../../packages-common/
cp target/akiban-client-*-SNAPSHOT.jar ../../packages-common/
cp target/dependency/postgresql.jar ../../packages-common/

# Windows
# Handled already by Maven / .iss
popd && popd

if [ -z "$2" ] ; then
	epoch=`date +%s`
else
	epoch=$2
fi

# Handle platform-specific packaging process
if [ ${platform} == "debian" ]; then
    cp packages-common/* ${platform}
    mvn -Dmaven.test.skip.exec clean install -DBZR_REVISION=${bzr_revno}
    debuild
elif [ ${platform} == "redhat" ]; then
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    tar_file=${PWD}/redhat/rpmbuild/SOURCES/akserver.tar
    bzr export --format=tar $tar_file
    rm {$PWD}/redhat/akserver/redhat/* # Clear out old files
    cp packages-common/* ${PWD}/redhat/akserver/redhat
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
elif [ ${platform} == "macosx" ]; then
    server_jar=target/akiban-server-1.3.0-jar-with-dependencies.jar
    akdump_jar=packages-common/akiban-client-tools-1.3.0-SNAPSHOT.jar
    postgres_jar=packages-common/postgresql.jar
    akdump_bin=packages-common/akdump
    mac_app='target/Akiban Server.app'
    mac_dmg='target/Akiban Server.dmg'
    inst_temp=/tmp/inst_temp

    # copy icon data from a "prototype" file
    tar xzf macosx/license-icon.tar.gz
    xattr -wx com.apple.FinderInfo "`xattr -px com.apple.FinderInfo prototype.txt`" ${license}
    cp prototype.txt/..namedfork/rsrc ${license}/..namedfork/rsrc
    rm prototype.txt
    
    # build jar
    mvn -Dmaven.test.skip.exec clean install -DBZR_REVISION=${bzr_revno}

    # add ce/ee specific config files to Contents/Resources/config
    rm macosx/Contents/Resources/config/*
    cp macosx/${target}/* macosx/Contents/Resources/config/
    # build app bundle
    mkdir "$mac_app"
    cp -R macosx/Contents "$mac_app"
    mkdir "$mac_app/Contents/MacOS"
    cp /System/Library/Frameworks/JavaVM.framework/Versions/Current/Resources/MacOS/JavaApplicationStub "$mac_app/Contents/MacOS"
    mkdir -p "$mac_app/Contents/Resources/Java"
    cp $server_jar "$mac_app/Contents/Resources/Java"
    mkdir -p "$mac_app/Contents/Resources/tools/"{lib,bin}
    cp $akdump_jar "$mac_app/Contents/Resources/tools/lib/"
    cp $postgres_jar "$mac_app/Contents/Resources/tools/lib/"
    cp $akdump_bin "$mac_app/Contents/Resources/tools/bin/"
    SetFile -a B "$mac_app"
    # build disk image template
    rm -rf $inst_temp; mkdir $inst_temp; mkdir "$inst_temp/Akiban Server.app"
    ln -s /Applications $inst_temp
    mkdir $inst_temp/.background
    cp macosx/dmg_background.png $inst_temp/.background
    rm -f $inst_temp.dmg
    hdiutil create -fs HFSX -layout SPUD -size 40m $inst_temp.dmg -format UDRW -volname 'Akiban Server' -srcfolder $inst_temp
    rm -rf $inst_temp
    # update disk image
    mkdir $inst_temp
    hdiutil attach $inst_temp.dmg -noautoopen -mountpoint $inst_temp
    ditto "$mac_app" "$inst_temp/Akiban Server.app"
    ${mac_ce_cmd}
    # == add non-app files here ==
    cp macosx/dmg.DS_Store $inst_temp/.DS_Store
    cp macosx/dmg_VolumeIcon.icns $inst_temp/.VolumeIcon.icns
    cp ${license} $inst_temp/LICENSE.txt
    SetFile -a C $inst_temp
    hdiutil detach `hdiutil info | grep $inst_temp | grep '^/dev' | cut -f1`
    hdiutil convert $inst_temp.dmg -format UDZO -imagekey zlib-level=9 -o "$mac_dmg"
    rm $inst_temp.dmg
else
    echo "Invalid Argument: ${platform}"
    echo "${usage}"
    exit 1
fi
