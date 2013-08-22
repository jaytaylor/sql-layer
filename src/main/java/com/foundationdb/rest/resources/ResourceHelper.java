/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.rest.resources;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.service.security.SecurityService;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.security.Principal;

public class ResourceHelper {
    public static final Response FORBIDDEN_RESPONSE = Response.status(Response.Status.FORBIDDEN).build();

    // Standard but not otherwise defined
    public static final String APPLICATION_JAVASCRIPT = "application/javascript";
    public static final MediaType APPLICATION_JAVASCRIPT_TYPE = MediaType.valueOf(APPLICATION_JAVASCRIPT);

    // For @Produces argument
    public static final String MEDIATYPE_JSON_JAVASCRIPT = MediaType.APPLICATION_JSON + "," + APPLICATION_JAVASCRIPT;

    public static final String JSONP_ARG_NAME = "callback";

    public static final String IDENTIFIERS_MULTI = "{identifiers:.*}";

    public static String getSchema(HttpServletRequest request) {
        Principal user = request.getUserPrincipal();
        return (user == null) ? "" : user.getName();
    }

    public static TableName parseTableName(HttpServletRequest request, String name) {
        String schema = getSchema(request);
        return TableName.parse(schema, name);
    }

    public static void checkTableAccessible(SecurityService security, HttpServletRequest request, TableName name) {
        checkSchemaAccessible(security, request, name.getSchemaName());
    }

    public static void checkSchemaAccessible(SecurityService security, HttpServletRequest request, String schema) {
        if(!security.isAccessible(request, schema)) {
            throw new WebApplicationException(FORBIDDEN_RESPONSE);
        }
    }

    /** Expected to be used along with {@link #IDENTIFIERS_MULTI} */
    public static String getPKString(UriInfo uri) {
        String pks[] = uri.getPath(false).split("/");
        assert pks.length > 0: uri;
        return pks[pks.length - 1];
    }
}
