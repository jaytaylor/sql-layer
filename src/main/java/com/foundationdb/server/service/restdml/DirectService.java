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

package com.foundationdb.server.service.restdml;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.session.Session;

public interface DirectService {

    public void installLibrary(PrintWriter writer, HttpServletRequest request, String module, String definition,
            String language) throws Exception;

    public void removeLibrary(PrintWriter writer, HttpServletRequest request, String module) throws Exception;

    public void reportStoredProcedures(PrintWriter writer, HttpServletRequest request, String query, String module,
            Session session, boolean functionsOnly) throws Exception;

    public DirectInvocation prepareRestInvocation(final String method, final TableName procName,
            final String pathParams, final MultivaluedMap<String, String> queryParameters, final byte[] content,
            final HttpServletRequest request) throws Exception;
        
    public void invokeRestEndpoint(final PrintWriter writer, final HttpServletRequest request, final String method,
            final DirectInvocation in) throws Exception;

}
