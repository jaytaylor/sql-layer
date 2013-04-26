/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.rest.resources;

import static com.akiban.rest.resources.ResourceHelper.JSONP_ARG_NAME;

import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.ClassXRefWriter;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.rest.RestResponseBuilder.BodyGenerator;
import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.service.restdml.DirectInvocation;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;

/**
 * Easy access to the server version
 */
@Path("/direct")
public class DirectResource {

    private final static String TABLE_ARG_NAME = "table";
    private final static String MODULE_ARG_NAME = "module";
    private final static String SCHEMA_ARG_NAME = "schema";
    private final static String LANGUAGE = "language";
    private final static String FUNCTIONS = "functions";

    private final ResourceRequirements reqs;

    public DirectResource(ResourceRequirements reqs) {
        this.reqs = reqs;
    }

    /**
     * Derive and return code-generated a set of Java interfaces describing
     * created from a schema. The supplied table is
     * 
     * @param request
     * @param table
     * @param jsonp
     * @return
     * @throws Exception
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/igen")
    public Response igen(@Context final HttpServletRequest request, @QueryParam(TABLE_ARG_NAME) final String table,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) throws Exception {

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName tableName = ResourceHelper.parseTableName(request, table == null ? "*" : table);
                final String schemaName = tableName.getSchemaName();
                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(schemaName) == null) {
                    throw new NoSuchSchemaException(schemaName);
                }
                ClassBuilder helper = new ClassSourceWriter(writer, false);
                helper.writeGeneratedInterfaces(ais, tableName);
                helper.close();
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/cgen")
    public Response cgen(@Context final HttpServletRequest request, @QueryParam(TABLE_ARG_NAME) final String table,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) throws Exception {

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName tableName = ResourceHelper.parseTableName(request, table == null ? "*" : table);
                final String schemaName = tableName.getSchemaName();
                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(schemaName) == null) {
                    throw new NoSuchSchemaException(schemaName);
                }
                ClassBuilder helper = new ClassSourceWriter(writer, false);
                helper.writeGeneratedClass(ais, tableName);
                helper.close();
            }
        }).build();
    }

    /**
     * Create a JSON-formatted set of nested arrays in the form <code><pre>
     * [className1,[
     *     [methodName1,returnType1],
     *     [methodName2,returnType2]],
     * className2,[
     *      ...]]
     * </pre></code This information can be used in an editor to support
     * context-specific code-completion. Note that:
     * <ul>
     * <li>a methodName with parentheses also includes formal parameter names
     * (which may be convenient for code-completion</li>
     * <li>a methodName without parentheses is actually a property name</li>
     * <li>returnType is either a className or is the empty string.</li>
     * </ul>
     * For example, ["getItem(
     * 
     * @param request
     * @param table
     * @param jsonp
     * @return
     * @throws Exception
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/xgen")
    public Response xgen(@Context final HttpServletRequest request, @QueryParam(TABLE_ARG_NAME) final String table,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) throws Exception {

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName tableName = ResourceHelper.parseTableName(request, table == null ? "*" : table);
                final String schemaName = tableName.getSchemaName();
                final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(
                        reqs.sessionService.createSession());
                if (ais.getSchema(schemaName) == null) {
                    throw new NoSuchSchemaException(schemaName);
                }
                ClassBuilder helper = new ClassXRefWriter(writer);
                helper.writeGeneratedXrefs(ais, tableName);
                helper.close();
            }
        }).build();
    }

    @PUT
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/library")
    public Response createProcedure(@Context final HttpServletRequest request,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("DefaultModule") final String module,
            @QueryParam(LANGUAGE) @DefaultValue("Javascript") final String language,
            @QueryParam(JSONP_ARG_NAME) final String jsonp, final String definition) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.directService.installLibrary(writer, request, module, definition, language);
            }
        }).build();
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/library")
    public Response deleteProcedure(@Context final HttpServletRequest request,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("DefaultModule") final String module,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.directService.removeLibrary(writer, request, module);
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/procedure")
    public Response getProcedures(@Context final HttpServletRequest request,
            @QueryParam(SCHEMA_ARG_NAME) @DefaultValue("") final String schema,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("") final String procName,
            @QueryParam(FUNCTIONS) @DefaultValue("false") final boolean functionsOnly) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                try (Session session = reqs.sessionService.createSession();
                        TransactionService.CloseableTransaction txn = reqs.transactionService
                                .beginCloseableTransaction(session)) {
                    reqs.directService.reportStoredProcedures(writer, request, schema, procName, session, functionsOnly);
                    txn.commit();
                }
            }
        }).build();
    }

    @GET
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callGet(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        
        return RestResponseBuilder.forRequest(request).body(new DirectBodyGenerator() {
            
            @Override
            public void prepare(Response.ResponseBuilder builder) {
                try {
                    invocation = reqs.directService.prepareRestInvocation("GET", procName, pathParams, uri.getQueryParameters(), null, request);
                    setHeaders(builder);
                } catch (Exception e) {
                    latentException = e;
                }
            }
            
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.directService.invokeRestEndpoint(
                        writer, request, "GET", invocation);
            }
        }).build();
    }

    @POST
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callPost(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri, final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        
        return RestResponseBuilder.forRequest(request).body(new DirectBodyGenerator() {
            
            @Override
            public void prepare(Response.ResponseBuilder builder) {
                try {
                    invocation = reqs.directService.prepareRestInvocation("POST", procName, pathParams, uri.getQueryParameters(), content, request);
                    setHeaders(builder);
                } catch (Exception e) {
                    latentException = e;
                }
            }
            
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.directService.invokeRestEndpoint(
                        writer, request, "POST", invocation);
            }
        }).build();
    }

    @PUT
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callPut(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri, final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        return RestResponseBuilder.forRequest(request).body(new DirectBodyGenerator() {

            @Override
            public void prepare(Response.ResponseBuilder builder) {
                try {
                    invocation = reqs.directService.prepareRestInvocation("PUT", procName, pathParams, uri.getQueryParameters(), content, request);
                    setHeaders(builder);
                } catch (Exception e) {
                    latentException = e;
                }
            }
            
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.directService.invokeRestEndpoint(
                        writer, request, "PUT", invocation);
            }
        }).build();
    }

    @DELETE
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callDelete(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri, final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        return RestResponseBuilder.forRequest(request).body(new DirectBodyGenerator() {

            @Override
            public void prepare(Response.ResponseBuilder builder) {
                try {
                    invocation = reqs.directService.prepareRestInvocation("POST", procName, pathParams, uri.getQueryParameters(), content, request);
                    setHeaders(builder);
                } catch (Exception e) {
                    latentException = e;
                }
            }
            
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.directService.invokeRestEndpoint(
                        writer, request, "DELETE", invocation);
            }
        }).build();

    }

    private abstract class DirectBodyGenerator extends BodyGenerator {
        
        DirectInvocation invocation;
        
        Exception latentException;
        
        @Override
        protected void throwPendingThrowable() throws Throwable {
            if (latentException != null) {
                throw latentException;
            }
        }
        
        @Override
        protected void finish() throws Exception {
            if (invocation != null) {
                invocation.finish();
            }
        }
        
        void setHeaders(Response.ResponseBuilder builder) {
            invocation.getEndpointMetadata().setResponseHeaders(builder);
        }
    }
}
