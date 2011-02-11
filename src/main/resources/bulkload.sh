#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#

#/bin/bash
set -x

# --akserver localhost:8080
# --mysql   localhost
# --user    root
# --temp    bulkload
# --group   coi
# --source  acme:acme_source

CLASSPATH=AKSERVER.INSTALL.DIR/usr/local/chunkserver/akiban-server-1.0-SNAPSHOT-jar-with-dependencies.jar

java  -ea -cp ${CLASSPATH} com.akiban.server.util.BulkLoaderClient  $*
