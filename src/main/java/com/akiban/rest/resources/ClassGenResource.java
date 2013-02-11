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
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
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
                    ClassBuilder helper = new ClassSourceWriter(new PrintWriter(output), "com.akiban.direct.schema",
                            schema, false, true);
                    generateSchema(helper, schema);
                } catch (InvalidOperationException e) {
                    throwToClient(e);
                }
            }
        }).build();
    }

    private void generateSchema(ClassBuilder helper, String schema) {
        final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(sessionService.createSession());
        helper.preamble(new String[] {"java.util.Date", "java.util.List"});
        helper.startClass(ClassSourceWriter.asJavaName(schema, true));
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            generateInterface(helper, table);
        }
        helper.end();
    }

    private void generateInterface(ClassBuilder helper, UserTable table) {
        table.getName().getTableName();
        String typeName = ClassSourceWriter.asJavaName(table.getName().getTableName(), true);
        helper.startClass(typeName);
        /*
         * Add a property per column
         */
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = column.getType().akType().javaClass();
            helper.addProperty(column.getName(), javaClass.getSimpleName(), null, null, null);
        }
        
        /*
         * Add an accessor for the parent row if there is one 
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            String parentTypeName = parentJoin.getParent().getName().getTableName();
            helper.addMethod("get" + ClassSourceWriter.asJavaName(parentTypeName, true),
                    ClassSourceWriter.asJavaName(parentTypeName, true), NONE, null, null);
        }
        
        /*
         * Add an accessor for each child table.
         */
        for (final Join join : table.getChildJoins()) {
            String childTypeName = join.getChild().getName().getTableName();
            helper.addMethod("get" + ClassSourceWriter.asJavaName(childTypeName, true), "List<" + ClassSourceWriter.asJavaName(childTypeName, true) + ">", NONE, null, null);
        }
        /*
         * Add boilerplate methods
         */
        helper.addMethod("copy", typeName, NONE, null, null);
        helper.addMethod("save", "void", NONE, null, null);
        
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
