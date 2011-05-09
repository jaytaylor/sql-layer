/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.server.service.Service;

import java.net.*;
import java.io.*;
import java.util.*;

/** The PostgreSQL server service.
 * @see PostgresServer
 * 
 * <code>JVM_OPTS="-Dakserver.services.customload=com.akiban.sql.pg.PostgresServerManager" $AKIBAN_SERVER_HOME/bin/akserver -f</code>
*/
public class PostgresServerManager implements PostgresService, Service<PostgresService> {
    private PostgresServer server = null;

    public PostgresServerManager() {
    }

    /*** Service<PostgresService> ***/

    public PostgresService cast() {
        return this;
    }

    public Class<PostgresService> castClass() {
        return PostgresService.class;
    }

    public void start() throws Exception {
        server = new PostgresServer(PostgresServer.DEFAULT_PORT);
        server.start();
    }

    public void stop() throws Exception {
        if (server != null) {
            server.stop();
            server = null;
        }
    }

    public void crash() throws Exception {
        stop();
    }

}
