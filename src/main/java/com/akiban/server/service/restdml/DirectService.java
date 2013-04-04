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

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import com.akiban.ais.model.TableName;

public interface DirectService {

    public void installLibrary(PrintWriter writer, HttpServletRequest request, String module, String definition, String language)
            throws Exception;

    public void removeLibrary(PrintWriter writer, final HttpServletRequest request, String module) throws Exception;

    public void invokeRestEndpoint(PrintWriter writer, HttpServletRequest request, String method, TableName procName,
            String pathParams, MultivaluedMap<String, String> queryParameters, byte[] content, MediaType[] responseType)
            throws Exception;

}
