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

import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.util.Arrays;

@Path("/sql")
public class SQLResource {
    private final ResourceRequirements reqs;

    public SQLResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    /** Run a single SQL statement specified by the 'q' query parameter. */
    @GET
    @Path("/query")
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(@Context final HttpServletRequest request,
                          @QueryParam("jsonp") String jsonp,
                          @QueryParam("q") final String query) {
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQL(writer, request, query);
                    }
                })
                .build();
    }

    /** Explain a single SQL statement specified by the 'q' query parameter. */
    @GET
    @Path("/explain")
    @Produces(MediaType.APPLICATION_JSON)
    public Response explain(@Context final HttpServletRequest request,
                            @QueryParam("jsonp") String jsonp,
                            @QueryParam("q") final String query) {
        return RestResponseBuilder
                .forJsonp(jsonp)
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
    @Produces(MediaType.APPLICATION_JSON)
    public Response execute(@Context final HttpServletRequest request,
                            @QueryParam("jsonp") String jsonp,
                            final byte[] postBytes) {
        String input = new String(postBytes);
        final String[] statements = input.split(";");
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQL(writer, request, Arrays.asList(statements));
                    }
                })
                .build();
    }
}
