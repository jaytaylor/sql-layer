/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
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
