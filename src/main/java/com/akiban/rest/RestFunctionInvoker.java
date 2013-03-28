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

package com.akiban.rest;

import java.io.PrintWriter;

import javax.ws.rs.core.MultivaluedMap;

import com.akiban.ais.model.TableName;
import com.akiban.sql.embedded.JDBCConnection;

public interface RestFunctionInvoker {

    public void invokeRestFunction(final PrintWriter writer, JDBCConnection conn, final String method,
            final TableName procName, final String pathParams, final MultivaluedMap<String, String> queryParameters,
            final byte[] content) throws Exception;
    
}
