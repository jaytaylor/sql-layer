
package com.akiban.rest.resources;

import com.akiban.ais.model.TableName;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;

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
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.callProcedure(writer, request, JSONP_ARG_NAME,
                                                          procName, uri.getQueryParameters());
                    }
                })
                .build();
    }

    @POST
    @Consumes(MEDIATYPE_JSON_JAVASCRIPT)
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response postCall(@Context final HttpServletRequest request,
                             @PathParam("proc") String proc,
                             final String jsonParams) throws Exception {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.callProcedure(writer, request, JSONP_ARG_NAME,
                                                          procName, jsonParams);
                    }
                })
                .build();
    }

}
