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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.foundationdb.rest.RestResponseBuilder;
import com.foundationdb.server.error.ErrorCode;

import static com.foundationdb.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

@Path("{other:.*}")
public class DefaultResource {

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handleGetNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }
    
    @PUT
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePutNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }
    
    @POST
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePostNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }
    
    @DELETE
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handleDeleteNoResource(@Context HttpServletRequest request) {
        return buildResponse(request);
    }

    @PATCH
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePatchNoResource(@Context HttpServletRequest request) {
        return buildResponse (request);
    }
    
    
    static Response buildResponse(HttpServletRequest request) {
        String msg = String.format("API %s not supported", request.getRequestURI());
        return RestResponseBuilder
                .forRequest(request)
                .status(Response.Status.NOT_FOUND)
                .body(ErrorCode.MALFORMED_REQUEST, msg)
                .build();
    }
    
    
}
