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

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.DirectClassLoader;
import com.akiban.direct.DirectModule;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.google.inject.Inject;

/**
 * Easy access to the server version
 */
@Path("direct/{op}/{schema}")
public class DirectResource {

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
        if ("interface".equals(op)) {
            /*
             * Generate Java interfaces in text form
             */
            return Response.status(Response.Status.OK).entity(new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException {
                    final AkibanInformationSchema ais = dxlService.ddlFunctions()
                            .getAIS(sessionService.createSession());
                    if (ais.getSchema(schema) == null) {
                        throw new RuntimeException("No such schema: " + schema);
                    }
                    ClassBuilder helper = new ClassSourceWriter(new PrintWriter(output), PACKAGE,
                            ClassBuilder.schemaClassName(schema), false);
                    try {
                        helper.writeGeneratedInterfaces(ais, schema);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).build();
        }
        
        if ("class".equals(op)) {
            /*
             * Generate Java interfaces in text form
             */
            return Response.status(Response.Status.OK).entity(new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException {
                    final AkibanInformationSchema ais = dxlService.ddlFunctions()
                            .getAIS(sessionService.createSession());
                    if (ais.getSchema(schema) == null) {
                        throw new RuntimeException("No such schema: " + schema);
                    }
                    ClassBuilder helper = new ClassSourceWriter(new PrintWriter(output), PACKAGE,
                            ClassBuilder.schemaClassName(schema), false);
                    try {
                        helper.writeGeneratedClass(ais, schema, params.getFirst("table"));
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
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
                @SuppressWarnings("resource")
                @Override
                public void write(OutputStream output) throws IOException {
                    try (Session session = sessionService.createSession()) {
                        final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(
                                sessionService.createSession());
                        if (ais.getSchema(schema) == null) {
                            throw new RuntimeException("No such schema: " + schema);
                        }
                        ClassBuilder.compileGeneratedInterfacesAndClasses(ais, schema);
                        final DirectClassLoader dcl = new DirectClassLoader(systemClassLoader());
                        final Class<? extends DirectModule> serviceClass = dcl.loadModule(ais, moduleName, urls);
                        DirectModule module = serviceClass.newInstance();
                        DirectModule replaced = dispatch.put(serviceClass.getSimpleName(), module);
                        if (replaced != null) {
                            ClassLoader cl  = replaced.getClass().getClassLoader();
                            assert cl instanceof DirectClassLoader;
                            ((DirectClassLoader)cl).close();
                        }
                    } catch (Exception e) {
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

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeModule(@PathParam("op") final String op, @PathParam("schema") final String schema,
            @Context final UriInfo uri) throws Exception {
        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        if ("load".equals(op)) {
            final String moduleName = params.getFirst("name");
            return Response.status(Response.Status.OK).entity(new StreamingOutput() {
                @Override
                public void write(OutputStream output) throws IOException {
                    
                    DirectModule removed = dispatch.remove(moduleName);
                    if (removed != null) {
                        ClassLoader cl  = removed.getClass().getClassLoader();
                        assert cl instanceof DirectClassLoader;
                        ((DirectClassLoader)cl).close();
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
    
    private ClassLoader systemClassLoader() {
        ClassLoader cl = getClass().getClassLoader();
        while (cl.getParent() != null && cl.getParent() != cl) {
            cl = cl.getParent();
        }
        return cl;
    }
}
