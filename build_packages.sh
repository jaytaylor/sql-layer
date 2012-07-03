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

if [ $# -lt 1 ]; then
    echo "Usage: ./build_packages.sh [debian|redhat|macosx]"
    exit 1
fi

platform=$1
bzr_revno=`bzr revno`

if [ -z "$2" ] ; then
	epoch=`date +%s`
else
	epoch=$2
fi

if [ ${platform} == "debian" ]; then
    cp packages-common/* ${platform}
    mvn -Dmaven.test.skip.exec clean install -DBZR_REVISION=${bzr_revno}
    debuild
elif [ ${platform} == "redhat" ]; then
    mkdir -p ${PWD}/redhat/rpmbuild/{BUILD,SOURCES,SRPMS,RPMS/noarch}
    tar_file=${PWD}/redhat/rpmbuild/SOURCES/akserver.tar
    bzr export --format=tar $tar_file
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
    server_jar=target/akiban-server-1.3.0-SNAPSHOT-jar-with-dependencies.jar
    mac_app='target/Akiban Server.app'
    mac_dmg='target/Akiban Server.dmg'
    inst_temp=/tmp/inst_temp
    # build jar
    mvn -Dmaven.test.skip.exec clean install -DBZR_REVISION=${bzr_revno}
    # build app bundle
    mkdir "$mac_app"
    cp -R macosx/Contents "$mac_app"
    mkdir "$mac_app/Contents/MacOS"
    cp /System/Library/Frameworks/JavaVM.framework/Versions/Current/Resources/MacOS/JavaApplicationStub "$mac_app/Contents/MacOS"
    mkdir "$mac_app/Contents/Java"
    cp $server_jar "$mac_app/Java"
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
    ditto "$mac_app" $inst_temp/Akiban Server.app
    cp macosx/dmg.DS_Store $inst_temp/.DS_Store
    cp macosx/dmg_VolumeIcon.icns $inst_temp/.VolumeIcon.icns    
    SetFile -a C $inst_temp
    hdiutil detach `hdiutil info | grep $inst_temp | grep '^/dev' | cut -f1`
    hdiutil convert $inst_temp.dmg -format UDZO -imagekey zlib-level=9 -o "$mac_dmg"
    rm $inst_temp.dmg
else
    echo "Invalid Argument: ${platform}"
    echo "Usage: ./build_packages.sh [debian|redhat]"
    exit 1
fi
