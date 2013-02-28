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

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javassist.CtClass;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.DirectClassLoader;
import com.akiban.direct.DirectContextImpl;
import com.akiban.direct.DirectException;
import com.akiban.direct.DirectModule;
import com.akiban.direct.script.JSModule;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.rest.RestResponseBuilder.BodyGenerator;
import com.akiban.server.service.session.Session;

/**
 * Easy access to the server version
 */
@Path("direct")
public class DirectResource {

    private final static String SPACE_ARG_NAME = "space";
    private final static String MODULE_ARG_NAME = "op";
    private final static String PACKAGE = "com.akiban.direct.entity";

    private static class DirectModuleHolder {
        DirectModule module;
        DirectContextImpl context;

        private DirectModuleHolder(final DirectModule module, final DirectContextImpl context) {
            this.module = module;
            this.context = context;
        }
    }

    private Map<String, DirectModuleHolder> dispatch = new HashMap<String, DirectModuleHolder>();
    private final ResourceRequirements reqs;

    public DirectResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }
    
    @GET
    @Produces(MediaType.WILDCARD)
    @Path("{file: demo/.+}")
    // TODO - remove this
    public Response getAsset(@PathParam("file") final String fileName) throws Exception {
        return RestResponseBuilder.forJsonp(null).body(new BodyGenerator() {

            @Override
            public void write(PrintWriter writer) throws Exception {
                String path = "direct-demo/" + fileName.substring("demo/".length());
                try (FileReader reader = new FileReader(path);) {
                    int c;
                    while ((c = reader.read()) != -1) {
                        writer.write(c);
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{op}")

    public Response get(@PathParam(MODULE_ARG_NAME) final String op, @QueryParam(SPACE_ARG_NAME) final String space,
            @Context final UriInfo uri, @QueryParam(JSONP_ARG_NAME) String jsonp) throws Exception {

        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        if ("igen".equals(op)) {
            /*
             * Generate Java interfaces in text form
             */
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {

                    final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                            reqs.sessionService.createSession());
                    if (ais.getSchema(space) == null) {
                        throw new RuntimeException("No such space: " + space);
                    }
                    ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, ClassBuilder.schemaClassName(space),
                            false);
                    try {
                        helper.writeGeneratedInterfaces(ais, space);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).build();
        }

        if ("cgen".equals(op)) {
            /*
             * Generate Java interfaces in text form
             */
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {
                    final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                            reqs.sessionService.createSession());
                    if (ais.getSchema(space) == null) {
                        throw new RuntimeException("No such space: " + space);
                    }
                    ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, ClassBuilder.schemaClassName(space),
                            false);
                    try {
                        helper.writeGeneratedClass(ais, space, params.getFirst("table"));
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }).build();
        }


        final DirectModuleHolder holder = dispatch.get(op);

        if (holder != null && holder.module.isGetEnabled()) {
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {
                    try {
                        writer.print(eval(holder, params));
                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        holder.context.leave();
                    }
                }
            }).build();

        }

        throw new NullPointerException("Module named " + op + " not loaded");
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{op}")
    public Response put(@PathParam(MODULE_ARG_NAME) final String op, @QueryParam(SPACE_ARG_NAME) final String space,
            @Context final UriInfo uri, @QueryParam(JSONP_ARG_NAME) String jsonp, final byte[] payload)
            throws Exception {
        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        if ("module".equals(op)) {
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {
                    loadModule(space, params, payload);
                }
            }).build();
        }

        final DirectModuleHolder holder = dispatch.get(op);
        if (holder.module != null && !holder.module.isGetEnabled()) {
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {
                    try {
                        writer.print(eval(holder, params));
                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }).build();
        }

        throw new NullPointerException("Module named " + op + " not loaded");
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{op}")
    public Response delete(@PathParam(MODULE_ARG_NAME) final String op,
            @QueryParam(SPACE_ARG_NAME) final String space, @Context final UriInfo uri,
            @QueryParam(JSONP_ARG_NAME) String jsonp) throws Exception {
        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        if ("module".equals(op)) {
            final String moduleName = params.getFirst("name");
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {
                    DirectModuleHolder holder = dispatch.remove(moduleName);
                    if (holder != null) {
                        unloadModule(holder);
                    }
                }
            }).build();
        }

        throw new NullPointerException("Module named " + op + " not loaded");
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{op}")
    public Response post(@PathParam(MODULE_ARG_NAME) final String op, @QueryParam(SPACE_ARG_NAME) final String space,
            @Context final UriInfo uri, @QueryParam(JSONP_ARG_NAME) String jsonp) throws Exception {
        final MultivaluedMap<String, String> params = uri.getQueryParameters();
        final DirectModuleHolder holder = dispatch.get(op);

        if (holder != null && !holder.module.isGetEnabled()) {
            return RestResponseBuilder.forJsonp(jsonp).body(new BodyGenerator() {

                @Override
                public void write(PrintWriter writer) throws Exception {
                    try {
                        writer.print(eval(holder, params));
                    } catch (Throwable e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        holder.context.leave();
                    }
                }
            }).build();
        }

        throw new NullPointerException("Module named " + op + " not loaded");
    }

    private void start(final DirectModule module, DirectContextImpl context, MultivaluedMap<String, String> params)
            throws Exception {
        module.setContext(context);
        /*
         * If module has optional method setParams, invoke it to pass the
         * parameters available at load time.
         */
        try {
            Method method = module.getClass().getMethod("setParams", Map.class);
            method.invoke(module, params);
        } catch (NoSuchMethodException e) {
            // ignore
        } catch (NoSuchMethodError | SecurityException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            throw e;
        }
        module.start();
    }

    private void stop(final DirectModule module) {
        try {
            module.stop();
        } catch (Exception e) {
            // ignore
        }
    }

    private Object eval(final DirectModuleHolder holder, MultivaluedMap<String, String> params) throws Exception {
        try {
            holder.context.enter();
            return holder.module.eval(params);
        } catch (DirectException e) {
            throw (Exception) e.getCause();
        } finally {
            holder.context.leave();
        }
    }

    private void loadModule(String space, MultivaluedMap<String, String> params, byte[] payload) throws Exception {
        String language = params.getFirst("language");
        String name = params.getFirst("name");
        String className = params.getFirst("class");
        List<String> urls = params.get("url");

        if ("js".equals(language)) {
            className = JSModule.class.getName();
            if (payload != null && payload.length > 0 && params.getFirst("source") == null) {
                params.put("source", Arrays.asList(new String(payload)));
            }
        }

        try (Session session = reqs.sessionService.createSession()) {
            final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                    reqs.sessionService.createSession());
            if (ais.getSchema(space) == null) {
                throw new RuntimeException("No such space: " + space);
            }
            Map<Integer, CtClass> generated = ClassBuilder.compileGeneratedInterfacesAndClasses(ais, space);
            final DirectClassLoader dcl = new DirectClassLoader(systemClassLoader());
            final Class<? extends DirectModule> serviceClass = dcl.loadModule(ais, className, urls);
            DirectModule module = serviceClass.newInstance();
            DirectContextImpl context = new DirectContextImpl(space, dcl);
            DirectModuleHolder holder = dispatch.put(name, new DirectModuleHolder(module, context));
            if (holder != null) {
                unloadModule(holder);
            }
            dcl.registerDirectObjectClasses(generated);
            start(module, context, params);
        }
    }

    private void unloadModule(DirectModuleHolder holder) throws IOException {
        ClassLoader cl = holder.module.getClass().getClassLoader();
        assert cl instanceof DirectClassLoader;
        stop(holder.module);
        ((DirectClassLoader) cl).close();
    }

    private ClassLoader systemClassLoader() {
        ClassLoader cl = getClass().getClassLoader();
        while (cl.getParent() != null && cl.getParent() != cl) {
            cl = cl.getParent();
        }
        return cl;
    }
}
