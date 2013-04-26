/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.service.restdml;

import java.sql.SQLException;

import com.akiban.ais.model.TableName;
import com.akiban.sql.embedded.JDBCConnection;


public class DirectInvocation {
    private final JDBCConnection conn;
    private final TableName procName;
    private final EndpointMetadata em;
    private final Object[] args;
    
    public DirectInvocation(JDBCConnection conn, TableName procName, EndpointMetadata em, Object[] args) {
        this.conn = conn;
        this.procName = procName;
        this.em = em;
        this.args = args;
    }

    public JDBCConnection getConnection() {
        return conn;
    }
    
    public TableName getProcName() {
        return procName;
    }
    
    public EndpointMetadata getEndpointMetadata() {
        return em;
    }

    public Object[] getArgs() {
        return args;
    }

    public void finish() {
        try {
            conn.close();
        } catch (SQLException e) {
            // probably the least of our problems.
        }
    }
}
