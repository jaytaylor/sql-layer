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

import static com.akiban.rest.resources.ResourceHelper.JSONP_ARG_NAME;
import static com.akiban.rest.resources.ResourceHelper.MEDIATYPE_JSON_JAVASCRIPT;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.rest.resources.ResourceHelper.parseTableName;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.akiban.ais.model.TableName;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;

@Path("/view/{view}")

public class ViewResource {
    private final ResourceRequirements reqs;
    
    public ViewResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MEDIATYPE_JSON_JAVASCRIPT)
    public Response retrieveEntity(@Context final HttpServletRequest request,
                                   @QueryParam(JSONP_ARG_NAME) String jsonp,
                                   @PathParam("view") String view,
                                   @QueryParam("depth") final Integer depth) {
        final TableName tableName = parseTableName(request, view);
        checkTableAccessible(reqs.securityService, request, tableName);
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ");
        query.append(tableName.toString());
        final String queryFinal = query.toString();
        
        return RestResponseBuilder
                .forJsonp(jsonp)
                .body(new RestResponseBuilder.BodyGenerator() {
                    @Override
                    public void write(PrintWriter writer) throws Exception {
                        reqs.restDMLService.runSQL(writer, request, queryFinal);
                    }
                })
                .build();
    }
}
