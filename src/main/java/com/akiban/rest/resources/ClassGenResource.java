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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.direct.ClassGenHelper;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.google.inject.Inject;

/**
 * Easy access to the server version
 */
@Path("/classgen")
public class ClassGenResource {

    private final static String[] NONE = new String[0];
    
    @Inject
    private DXLService dxlService;

    @Inject
    private SessionService sessionService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getVersion(@QueryParam("schema") final String schema) throws Exception {
        return Response.status(Response.Status.OK).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException {
                try (Session session = sessionService.createSession()) {
                    // Do not auto-close writer as that prevents an exception
                    // from propagating to the client
                    ClassGenHelper helper = new ClassGenHelper(new PrintWriter(output), "com.akiban.direct.schema",
                            schema);
                    generateSchema(helper, schema);
                    helper.newLine();
                    helper.close();
                } catch (InvalidOperationException e) {
                    throwToClient(e);
                }
            }
        }).build();
    }

    private void generateSchema(ClassGenHelper helper, String schema) {
        final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(sessionService.createSession());
        helper.preamble(NONE);
        helper.startInterface(ClassGenHelper.asJavaName(schema, true));
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            generateInterface(helper, table);
        }
        helper.end();
    }

    private void generateInterface(ClassGenHelper helper, UserTable table) {
        table.getName().getTableName();
        helper.newLine();
        helper.startInterface(ClassGenHelper.asJavaName(table.getName().getTableName(), true));
        /*
         * Add a property per column
         */
        for (final Column column : table.getColumns()) {
            helper.newLine();
            Class<?> javaClass = column.getType().akType().javaClass();
            helper.property(column.getName(), javaClass.getSimpleName());
        }
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            helper.newLine();
            String parentTypeName = parentJoin.getParent().getName().getTableName();
            helper.method("get" + ClassGenHelper.asJavaName(parentTypeName, true),
                    ClassGenHelper.asJavaName(parentTypeName, true), NONE);
        }
        
        for (final Join join : table.getChildJoins()) {
            helper.newLine();
            String childTypeName = join.getChild().getName().getTableName();
            helper.method("get" + ClassGenHelper.asJavaName(childTypeName, true), "Iterable<" + ClassGenHelper.asJavaName(childTypeName, true) + ">", NONE);
        }
        helper.end();
    }

    private void throwToClient(InvalidOperationException e) {
        StringBuilder err = new StringBuilder(100);
        err.append("[{\"code\":\"");
        err.append(e.getCode().getFormattedValue());
        err.append("\",\"message\":\"");
        err.append(e.getMessage());
        err.append("\"}]\n");
        // TODO: Map various IOEs to other codes?
        final Response.Status status;
        if (e instanceof NoSuchTableException) {
            status = Response.Status.NOT_FOUND;
        } else {
            status = Response.Status.INTERNAL_SERVER_ERROR;
        }
        throw new WebApplicationException(Response.status(status).entity(err.toString()).build());
    }
}
