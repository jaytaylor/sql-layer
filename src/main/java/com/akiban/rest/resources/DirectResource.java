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

import javassist.ClassPool;

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
import com.akiban.direct.ClassObjectWriter;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.DaoInterfaceBuilder;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.google.inject.Inject;

/**
 * Easy access to the server version
 */
@Path("/direct")
public class DirectResource {

    private final static String[] NONE = new String[0];
    private final static String PACKAGE = "com.akiban.direct.entity";

    @Inject
    DXLService dxlService;

    @Inject
    SessionService sessionService;

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getVersion(@QueryParam("schema") final String schema) throws Exception {
        return Response.status(Response.Status.OK).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException {
                try (Session session = sessionService.createSession()) {
                    final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(sessionService.createSession());
                    ClassBuilder helper = new ClassSourceWriter(new PrintWriter(output), PACKAGE, schema, false, true);
                    new DaoInterfaceBuilder().generateSchema(helper, ais, schema);
                } catch (InvalidOperationException e) {
                    throwToClient(e);
                }
            }
        }).build();
    }

    private void generateSchema(ClassBuilder helper, String schema) {
        final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(sessionService.createSession());
        helper.preamble(new String[] { "java.util.Date", "java.util.List" });
        String schemaAsClassName = PACKAGE + "." + ClassBuilder.asJavaName(schema, true);
        helper.startClass(schemaAsClassName);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            generateInterface(helper, table, schemaAsClassName);
        }
        helper.end();
        ClassPool pool = new ClassPool(true);
        ClassObjectWriter helper2 = new ClassObjectWriter(pool, PACKAGE, schema);
        helper2.preamble(new String[] { "java.util.Date", "java.util.List" });
        helper2.startClass(schemaAsClassName);
        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
            generateInterface(helper2, table, schemaAsClassName);
        }
        helper2.end();
    }

    private void generateInterface(ClassBuilder helper, UserTable table, String schemaAsClassName) {
        table.getName().getTableName();
        String typeName = schemaAsClassName + "$" + ClassBuilder.asJavaName(table.getName().getTableName(), true);
        helper.startClass(typeName);
        /*
         * Add a property per column
         */
        for (final Column column : table.getColumns()) {
            Class<?> javaClass = column.getType().akType().javaClass();
            helper.addProperty(column.getName(), javaClass.getName(), null, null, null);
        }

        /*
         * Add an accessor for the parent row if there is one
         */
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            String parentTypeName = parentJoin.getParent().getName().getTableName();
            helper.addMethod("get" + ClassBuilder.asJavaName(parentTypeName, true),
                    ClassBuilder.asJavaName(parentTypeName, true), NONE, null, null);
        }

        /*
         * Add an accessor for each child table.
         */
        for (final Join join : table.getChildJoins()) {
            String childTypeName = join.getChild().getName().getTableName();
            helper.addMethod("get" + ClassBuilder.asJavaName(childTypeName, true),
                    "List<" + ClassBuilder.asJavaName(childTypeName, true) + ">", NONE, null, null);
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
