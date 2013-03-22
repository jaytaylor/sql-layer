package com.akiban.rest.resources;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.server.error.ErrorCode;

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

@Path("{other:.*}")
public class DefaultResource {

    private final ResourceRequirements reqs;
    
    public DefaultResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handleGetNoResource(@Context HttpServletRequest request,
                                        @PathParam("other") String other) {
        return buildResponse(request, other);
    }
    
    @PUT
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePutNoResource(@Context HttpServletRequest request,
            @PathParam("other") String other) {
        return buildResponse(request, other);
    }
    
    @POST
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handlePostNoResource(@Context HttpServletRequest request,
            @PathParam("other") String other) {
        return buildResponse(request, other);
    }
    
    @DELETE
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response handleDeleteNoResource(@Context HttpServletRequest request,
            @PathParam("other") String other) {
        return buildResponse(request, other);
    }

    private Response buildResponse(HttpServletRequest request, String path) {
        String msg = String.format("API %s/%s not supported", reqs.restService.getContextPath(), path);
        return RestResponseBuilder
                .forRequest(request)
                .status(Response.Status.NOT_FOUND)
                .body(ErrorCode.MALFORMED_REQUEST, msg)
                .build();
    }
    
    
}
