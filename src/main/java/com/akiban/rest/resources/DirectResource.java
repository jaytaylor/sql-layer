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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.ClassPool;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassObjectWriter;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.DaoInterfaceBuilder;
import com.akiban.direct.DirectClassLoader;
import com.akiban.direct.DirectModule;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.google.inject.Inject;

/**
 * Easy access to the server version
 */
@Path("direct/{op}/{schema}")
public class DirectResource {

    private final static String[] NONE = new String[0];

    private final static String PACKAGE = "com.akiban.direct.entity";

    private Map<String, DirectModule> dispatch = new HashMap<String, DirectModule>();

    @Inject
    DXLService dxlService;

    @Inject
    SessionService sessionService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getInterfaceText(@PathParam("op") final String op, @PathParam("schema") final String schema,
            @Context final UriInfo uri) throws Exception {

        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        if ("dao".equals(op)) {
            /*
             * Generate Java interfaces in text form
             */
            return Response.status(Response.Status.OK).entity(new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException {
                    try (Session session = sessionService.createSession()) {
                        final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(
                                sessionService.createSession());
                        if (ais.getSchema(schema) == null) {
                            throw new RuntimeException("No such schema: " + schema);
                        }
                        ClassBuilder helper = new ClassSourceWriter(new PrintWriter(output), PACKAGE, schemaClassName(schema), false);
                        new DaoInterfaceBuilder().generateSchema(helper, ais, schema);
                    } catch (InvalidOperationException e) {
                        throwToClient(e);
                    }
                }
            }).build();
        }

        final DirectModule module = dispatch.get(op);
        if (module != null && module.isIdempotent()) {
            return Response.status(Response.Status.OK).entity(module.exec(params)).build();
        }

        throw new NullPointerException("Module named " + op + " not loaded");
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    public Response loadModule(@PathParam("op") final String op, @PathParam("schema") final String schema,
            @Context final UriInfo uri) throws Exception {
        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        if ("load".equals(op)) {
            final String moduleName = params.getFirst("name");
            final List<String> urls = params.get("url");
            return Response.status(Response.Status.OK).entity(new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException {
                    DirectClassLoader dcl = null;
                    try (Session session = sessionService.createSession()) {
                        final AkibanInformationSchema ais = dxlService.ddlFunctions()
                                .getAIS(sessionService.createSession());
                        if (ais.getSchema(schema) == null) {
                            throw new RuntimeException("No such schema: " + schema);
                        }
                        
                        /*
                         * Precompile the interfaces
                         */
                        ClassPool pool = new ClassPool(true);
                        ClassObjectWriter helper = new ClassObjectWriter(pool, PACKAGE, schema);
                        helper.preamble(new String[] { "java.util.Date", "java.util.List" });
                        String scn = schemaClassName(schema);
                        helper.startClass(scn, true);
                        for (final UserTable table : ais.getSchema(schema).getUserTables().values()) {
                            generateInterface(helper, table, scn);
                        }
                        helper.end();
                        
                        dcl = new DirectClassLoader(getClass().getClassLoader(), getClass().getClassLoader(), pool);
                        final Class<? extends DirectModule> serviceClass = dcl.loadModule(ais, moduleName, urls);
                        DirectModule module = serviceClass.newInstance();
                        dispatch.put(serviceClass.getSimpleName(), module);
                    } catch (InvalidOperationException e) {
                        throwToClient(e);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }).build();
        }
        
        final DirectModule module = dispatch.get(op);
        if (module != null && !module.isIdempotent()) {
            return Response.status(Response.Status.OK).entity(module.exec(params)).build();
        }

        throw new NullPointerException("Module named " + op + " not loaded");
    }


    private void generateInterface(ClassBuilder helper, UserTable table, String schemaAsClassName) {
        table.getName().getTableName();
        String typeName = schemaAsClassName + "$" + ClassBuilder.asJavaName(table.getName().getTableName(), true);
        helper.startClass(typeName, true);
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
                    "java.util.List<" + ClassBuilder.asJavaName(childTypeName, true) + ">", NONE, null, null);
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

    private String schemaClassName(String schema) {
        return PACKAGE + "." + ClassBuilder.asJavaName(schema, true);
    }
    

}
