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

package com.akiban.rest.resources;

import com.akiban.ais.model.TableName;
import com.akiban.http.SimpleHandlerList;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.util.tap.InOutTap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.PrintWriter;

import static com.akiban.rest.resources.ResourceHelper.JSONP_ARG_NAME;
import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

/**
 * Allows calling stored procedures directly.
 */
@Path("/call/{proc}")
public class ProcedureCallResource {
    private final ResourceRequirements reqs;
    private static final InOutTap CALL_GET = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: call GET");
    private static final InOutTap CALL_POST = SimpleHandlerList.REST_TAP.createSubsidiaryTap("rest: call POST");
    
    public ProcedureCallResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response getCall(@Context final HttpServletRequest request,
                            @PathParam("proc") String proc,
                            @Context final UriInfo uri) throws Exception {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        CALL_GET.in();
        try {
            return RestResponseBuilder
                    .forRequest(request)
                    .body(new RestResponseBuilder.BodyGenerator() {
                        @Override
                        public void write(PrintWriter writer) throws Exception {
                            reqs.restDMLService.callProcedure(writer, request, JSONP_ARG_NAME,
                                                              procName, uri.getQueryParameters(), null);
                        }
                    })
                    .build();
        } finally {
            CALL_GET.out();
        }
    }

    @POST
    @Consumes(MEDIATYPE_JSON_JAVASCRIPT)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response postCall(@Context final HttpServletRequest request,
                             @PathParam("proc") String proc,
                             @Context final UriInfo uri,
                             final String jsonParams) throws Exception {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        CALL_POST.in();
        try {
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
        } finally {
            CALL_POST.out();
        }
    }

}
