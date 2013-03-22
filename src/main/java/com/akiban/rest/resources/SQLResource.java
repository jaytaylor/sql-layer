
package com.akiban.rest.resources;

import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.util.Arrays;

import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;

@Path("/sql")
public class SQLResource {
    private final ResourceRequirements reqs;

    public SQLResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    /** Run a single SQL statement specified by the 'q' query parameter. */
    @GET
    @Path("/query")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response query(@Context final HttpServletRequest request,
                          @QueryParam("q") final String query) {
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQL(writer, request, query, null);
                    }
                })
                .build();
    }

    /** Explain a single SQL statement specified by the 'q' query parameter. */
    @GET
    @Path("/explain")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response explain(@Context final HttpServletRequest request,
                            @QueryParam("q") final String query) {
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.explainSQL(writer, request, query);
                    }
                })
                .build();
    }

    /** Run multiple SQL statements (single transaction) specified by semi-colon separated strings in the POST body. */
    @POST
    @Path("/execute")
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response execute(@Context final HttpServletRequest request,
                            final byte[] postBytes) {
        String input = new String(postBytes);
        final String[] statements = input.split(";");
        return RestResponseBuilder
                .forRequest(request)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQL(writer, request, Arrays.asList(statements));
                    }
                })
                .build();
    }
}
