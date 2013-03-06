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

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.rest.RestResponseBuilder.BodyGenerator;

/**
 * Easy access to the server version
 */
@Path("/direct")
public class DirectResource {

    private final static String SPACE_ARG_NAME = "space";
    private final static String TABLE_ARG_NAME = "table";
    private final static String PACKAGE = "com.akiban.direct.entity";

    private final ResourceRequirements reqs;

    public DirectResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/igen")
    public Response get(@Context HttpServletRequest request, @QueryParam(SPACE_ARG_NAME) final String space,
            @QueryParam(JSONP_ARG_NAME) String jsonp) throws Exception {

        /*
         * Generate Java interfaces in text form
         */
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {

            @Override
            public void write(PrintWriter writer) throws Exception {

                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(space) == null) {
                    throw new RuntimeException("No such space: " + space);
                }
                ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, ClassBuilder.schemaClassName(space), false);
                try {
                    helper.writeGeneratedInterfaces(ais, space);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/cgen")
    public Response cgen(@Context HttpServletRequest request, @QueryParam(SPACE_ARG_NAME) final String space,
            @QueryParam(TABLE_ARG_NAME) final String tableName, @QueryParam(JSONP_ARG_NAME) String jsonp)
            throws Exception {

        /*
         * Generate Java interfaces in text form
         */
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {

            @Override
            public void write(PrintWriter writer) throws Exception {
                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(space) == null) {
                    throw new RuntimeException("No such space: " + space);
                }
                ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, ClassBuilder.schemaClassName(space), false);
                try {
                    helper.writeGeneratedClass(ais, space, tableName);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }).build();
    }

}
