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
import com.foundationdb.rest.ResourceRequirements;
import com.foundationdb.rest.RestResponseBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.PrintWriter;

import static com.foundationdb.rest.resources.ResourceHelper.JSONP_ARG_NAME;
import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

/**
 * Allows calling stored procedures directly.
 */
@Path("/call/{proc}")
public class ProcedureCallResource {
    private final ResourceRequirements reqs;
    
    public ProcedureCallResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @POST
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response postCallJson(@Context final HttpServletRequest request,
                             @PathParam("proc") String proc,
                             @Context final UriInfo uri,
                             final String jsonParams) throws Exception {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.callProcedure(writer, request, JSONP_ARG_NAME,
                                                          procName, uri.getQueryParameters(), jsonParams);
                    }
                })
                .build();
    }
}
