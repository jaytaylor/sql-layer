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
import static com.akiban.rest.resources.ResourceHelper.checkSchemaAccessible;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.util.JsonUtils.createJsonGenerator;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.JsonGenerator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Routine.CallingConvention;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.TableName;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.ClassXRefWriter;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestFunctionInvoker;
import com.akiban.rest.RestFunctionRegistrar;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.rest.RestResponseBuilder.BodyGenerator;
import com.akiban.rest.RestServiceImpl;
import com.akiban.rest.resources.EndpointMetadata.EndpointAddress;
import com.akiban.rest.resources.EndpointMetadata.ParamCache;
import com.akiban.rest.resources.EndpointMetadata.ParamMetadata;
import com.akiban.rest.resources.EndpointMetadata.Tokenizer;
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.server.error.MalformedRequestException;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.service.routines.ScriptInvoker;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.embedded.JDBCConnection;

/**
 * Easy access to the server version
 */
@Path("/direct")
public class DirectResource implements RestFunctionInvoker {

    private final static String TABLE_ARG_NAME = "table";
    private final static String MODULE_ARG_NAME = "module";
    private final static String SCHEMA_ARG_NAME = "schema";
    private final static String LANGUAGE = "language";

    private final static String CALLING_CONVENTION = "calling_convention";
    private final static String MAX_DYNAMIC_RESULT_SETS = "max_dynamic_result_sets";
    private final static String DEFINITION = "definition";
    private final static String PARAMETERS_IN = "parameters_in";
    private final static String PARAMETERS_OUT = "parameters_out";
    private final static String NAME = "name";
    private final static String POSITION = "position";
    private final static String TYPE = "type";
    private final static String TYPE_OPTIONS = "type_options";
    private final static String IS_INOUT = "is_inout";
    private final static String IS_RESULT = "is_result";

    private final static String COMMENT_ANNOTATION1 = "//##";
    private final static String COMMENT_ANNOTATION2 = "##//";
    private final static String ENDPOINT = "endpoint";

    private final static String DISTINGUISHED_REGISTRATION_METHOD_NAME = "_register";

    private final static String CREATE_PROCEDURE_FORMAT = "CREATE PROCEDURE \"%s\" ()"
            + " LANGUAGE %s PARAMETER STYLE LIBRARY AS $$%s$$";

    private final static String DROP_PROCEDURE_FORMAT = "DROP PROCEDURE %s";

    private final static Object ENDPOINT_MAP_CACHE_KEY = new Object();

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
    @Path("/procedure")
    public Response createProcedure(@Context final HttpServletRequest request,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("DefaultModule") final String module,
            @QueryParam(LANGUAGE) @DefaultValue("Javascript") final String language,
            @QueryParam(JSONP_ARG_NAME) final String jsonp, final byte[] payload) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName procName = ResourceHelper.parseTableName(request, module);
                final String sql = String.format(CREATE_PROCEDURE_FORMAT, procName, language, new String(payload));
                reqs.restDMLService.runSQL(writer, request, sql, procName.getSchemaName());
            }
        }).build();
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/procedure")
    public Response deleteProcedure(@Context final HttpServletRequest request,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("DefaultModule") final String module,
            @QueryParam(JSONP_ARG_NAME) final String jsonp) {
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                final TableName procName = ResourceHelper.parseTableName(request, module);
                final String sql = String.format(DROP_PROCEDURE_FORMAT, procName);
                reqs.restDMLService.runSQL(writer, request, sql, procName.getSchemaName());
            }
        }).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/procedure/")
    public Response getProcedures(@Context final HttpServletRequest request,
            @QueryParam(SCHEMA_ARG_NAME) @DefaultValue("") final String schema,
            @QueryParam(MODULE_ARG_NAME) @DefaultValue("") final String procName) {
        final String schemaResolved = schema.isEmpty() ? ResourceHelper.getSchema(request) : schema;
        if (procName.isEmpty())
            checkSchemaAccessible(reqs.securityService, request, schemaResolved);
        else
            checkTableAccessible(reqs.securityService, request, new TableName(schemaResolved, procName));
        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                try (Session session = reqs.sessionService.createSession();
                        TransactionService.CloseableTransaction txn = reqs.transactionService
                                .beginCloseableTransaction(session)) {
                    JsonGenerator json = createJsonGenerator(writer);
                    AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(session);

                    if (procName.isEmpty()) {
                        // Get all routines in the schema.
                        json.writeStartObject();
                        {
                            Schema schemaAIS = ais.getSchema(schemaResolved);
                            if (schemaAIS != null) {
                                for (Map.Entry<String, Routine> routineEntry : schemaAIS.getRoutines().entrySet()) {
                                    json.writeFieldName(routineEntry.getKey());
                                    writeProc(routineEntry.getValue(), json);
                                }
                            }
                        }
                        json.writeEndObject();
                    } else {
                        // Get just the one routine.
                        Routine routine = ais.getRoutine(schemaResolved, procName);
                        if (routine == null)
                            throw new NoSuchRoutineException(schemaResolved, procName);
                        writeProc(routine, json);
                    }
                    json.flush();
                    txn.commit();
                }
            }
        }).build();
    }

    private void writeProc(Routine routine, JsonGenerator json) throws IOException {
        json.writeStartObject();
        {
            json.writeStringField(LANGUAGE, routine.getLanguage());
            json.writeStringField(CALLING_CONVENTION, routine.getCallingConvention().name());
            json.writeNumberField(MAX_DYNAMIC_RESULT_SETS, routine.getDynamicResultSets());
            json.writeStringField(DEFINITION, routine.getDefinition());
            writeProcParams(PARAMETERS_IN, routine.getParameters(), Parameter.Direction.IN, json);
            writeProcParams(PARAMETERS_OUT, routine.getParameters(), Parameter.Direction.OUT, json);
        }
        json.writeEndObject();
    }

    private void writeProcParams(String label, List<Parameter> parameters, Parameter.Direction direction,
            JsonGenerator json) throws IOException {
        json.writeArrayFieldStart(label);
        {
            for (int i = 0; i < parameters.size(); i++) {
                Parameter param = parameters.get(i);
                Parameter.Direction paramDir = param.getDirection();
                final boolean isInteresting;
                switch (paramDir) {
                case RETURN:
                    paramDir = Parameter.Direction.OUT;
                case IN:
                case OUT:
                    isInteresting = (paramDir == direction);
                    break;
                case INOUT:
                    isInteresting = true;
                    break;
                default:
                    throw new IllegalStateException("don't know how to handle parameter " + param);
                }
                if (isInteresting) {
                    json.writeStartObject();
                    {
                        json.writeStringField(NAME, param.getName());
                        json.writeNumberField(POSITION, i);
                        TInstance tInstance = param.tInstance();
                        TClass tClass = param.tInstance().typeClass();
                        json.writeStringField(TYPE, tClass.name().unqualifiedName());
                        json.writeObjectFieldStart(TYPE_OPTIONS);
                        {
                            for (Attribute attr : tClass.attributes())
                                json.writeObjectField(attr.name().toLowerCase(), tInstance.attributeToObject(attr));
                        }
                        json.writeEndObject();
                        json.writeBooleanField(IS_INOUT, paramDir == Parameter.Direction.INOUT);
                        json.writeBooleanField(IS_RESULT, param.getDirection() == Parameter.Direction.RETURN);
                    }
                    json.writeEndObject();
                }
            }
        }
        json.writeEndArray();
    }

    @GET
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callGet(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        final MediaType[] responseType = new MediaType[1];

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, "GET", procName, pathParams,
                        uri.getQueryParameters(), null, DirectResource.this, responseType);
            }
        }).build(responseType[0]);
    }

    @POST
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callPost(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri, final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        final MediaType[] responseType = new MediaType[1];

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, "POST", procName, pathParams,
                        uri.getQueryParameters(), content, DirectResource.this, responseType);
            }
        }).build(responseType[0]);
    }

    @PUT
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callPut(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri, final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        final MediaType[] responseType = new MediaType[1];

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, "PUT", procName, pathParams,
                        uri.getQueryParameters(), content, DirectResource.this, responseType);
            }
        }).build(responseType[0]);
    }

    @DELETE
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callDelete(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams, @Context final UriInfo uri, final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());
        final MediaType[] responseType = new MediaType[1];

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, "DELETE", procName, pathParams,
                        uri.getQueryParameters(), content, DirectResource.this, responseType);
            }
        }).build(responseType[0]);
    }

    /**
     * Invokes a function in a script library. The identity of the function is
     * determined from multiple factors including the schema name, the library
     * routine name, the URI of the request, the content type of the request,
     * etc. This method is called by {@link RestServiceImpl} which validates
     * security and supplies the JDBCConnection
     */
    @Override
    public void invokeRestFunction(final PrintWriter writer, JDBCConnection conn, final String method,
            final TableName procName, final String pathParams, final MultivaluedMap<String, String> queryParameters,
            final byte[] content, final String requestType, final MediaType[] responseType) throws Exception {

        ParamCache cache = new ParamCache();
        final EndpointMap endpointMap = getEndpointMap(conn.getSession());
        final List<EndpointMetadata> list;
        synchronized (endpointMap) {
            list = endpointMap.getMap().get(new EndpointAddress(method, procName));
        }

        EndpointMetadata md = selectEndpoint(list, pathParams, requestType, responseType, cache);
        if (md == null) {
            // TODO - Is this the correct Exception? Is there a way to convey
            // this without logged stack trace?
            throw new MalformedRequestException("No matching endpoint");
        }

        final Object[] args = createArgsArray(pathParams, queryParameters, content, cache, md);

        final ScriptInvoker invoker = conn.getRoutineLoader()
                .getScriptInvoker(conn.getSession(), new TableName(procName.getSchemaName(), md.routineName)).get();
        Object result = invoker.invokeNamedFunction(md.function, args);
        
        switch (md.outParam.type) {
        
        case EndpointMetadata.X_TYPE_STRING:
            responseType[0] = MediaType.TEXT_PLAIN_TYPE;
            if (result != null) {
                writer.write(result.toString());
            } else if (md.outParam.defaultValue != null) {
                writer.write(md.outParam.defaultValue.toString());
            }
            break;

        case EndpointMetadata.X_TYPE_JSON:
            responseType[0] = MediaType.APPLICATION_JSON_TYPE;
            if (result != null) {
                writer.write(result.toString());
            } else if (md.outParam.defaultValue != null) {
                writer.write(md.outParam.defaultValue.toString());
            }
            break;

        case EndpointMetadata.X_TYPE_BYTEARRAY:
            responseType[0] = MediaType.APPLICATION_OCTET_STREAM_TYPE;
            // TODO: Unsupported - need to add a path for writing a stream
            break;

        default:
            // No response type specified
            responseType[0] = null;
            break;
        }
    }

    /**
     * Select a registered <code>EndpointMetadata</code> from the supplied list.
     * The first candidate in the list that matches the end-point pattern
     * definition and has a compatible request content type is selected. If
     * there are no matching endpoints this method return <code>null</code>.
     * 
     * @param list
     *            List of <code>EndpointMetadata</code> instances to chose from.
     * @param pathParams
     *            String containing any path parameters; empty if none. If the
     *            value is not empty, its first character is '/'.
     * @param requestType
     *            MIME type specified in the request
     * @param responseType
     *            One-long array in which the response MIME type that will be
     *            generated by the matching endpoint will be returned
     * @param cache
     *            A <code>ParamCache</code> instance in which partial results
     *            are cached
     * @return the selected <code>EndpointMetadata</code> or <code>null</code>.
     */
    EndpointMetadata selectEndpoint(final List<EndpointMetadata> list, final String pathParams,
            final String requestType, final MediaType[] responseType, ParamCache cache) {
        EndpointMetadata md = null;
        if (list != null) {
            for (final EndpointMetadata candidate : list) {
                if (candidate.pattern != null) {
                    Matcher matcher = candidate.getParamPathMatcher(cache, pathParams);
                    if (matcher.matches()) {
                        if (responseType != null && candidate.expectedContentType != null
                                && !requestType.startsWith(candidate.expectedContentType)) {
                            continue;
                        }
                        md = candidate;
                        break;
                    }
                } else {
                    if (pathParams == null || pathParams.isEmpty()) {
                        md = candidate;
                        break;
                    }
                }
            }
        }
        return md;
    }

    /**
     * Construct an argument array in preparations for calling a function.
     * Values of the arguments are extracted from elements of the REST request,
     * including a portion of the URI containing parameter values (the
     * <code>pathParams</code>), <code>queryParams</code> specified by text
     * after a '?' character in the URI, and the content of the request which
     * may be interpreted as a byte array, a String or a JSON-formatted string
     * in which elements can be specified by name.
     * 
     * @param pathParams
     *            String containing parameters specified as part of the URI path
     * @param queryParameters
     *            <code>MultivaluedMap</code> containing query parameters
     * @param content
     *            Content of the request body as a byte array, or
     *            <code>null</code> in the case of a GET request.
     * @param cache
     *            A cache for partial results
     * @param md
     *            The <code>EndpointMetadata</code> instance selected by
     *            {@link #selectEndpoint(List, String, String, MediaType[], ParamCache)}
     * @return the argument array
     * @throws Exception
     */
    Object[] createArgsArray(final String pathParams, final MultivaluedMap<String, String> queryParameters,
            final byte[] content, ParamCache cache, EndpointMetadata md) throws Exception {
        final Object[] args = new Object[md.inParams.length];
        for (int index = 0; index < md.inParams.length; index++) {
            final ParamMetadata pm = md.inParams[index];
            Object v = pm.source.value(pathParams, queryParameters, content, cache);
            args[index] = EndpointMetadata.convertType(pm, v);
        }
        return args;
    }

    static class EndpointMap {
        final Map<EndpointAddress, List<EndpointMetadata>> map = new HashMap<EndpointAddress, List<EndpointMetadata>>();
        final ResourceRequirements reqs;
        Map<EndpointAddress, List<EndpointMetadata>> getMap() {
            return map;
        }
        
        EndpointMap(final ResourceRequirements reqs) {
            this.reqs = reqs;
        }

        void populate(final AkibanInformationSchema ais, final Session session) {
            for (final Routine routine : ais.getRoutines().values()) {
                if (routine.getCallingConvention().equals(CallingConvention.SCRIPT_LIBRARY)
                        && routine.getDynamicResultSets() == 0 && routine.getParameters().isEmpty()) {
                    final String definition = routine.getDefinition();
                    final String schemaName = routine.getName().getSchemaName();
                    final String procName = routine.getName().getTableName();
                    try {
                        parseAnnotations(schemaName, procName, definition);
                    } catch (Exception e) {
                        // Failed due to parse error on annotations.
                        // TODO - log and report this
                    }
                    try {
                        final ScriptInvoker invoker = reqs.routineLoader.getScriptInvoker(session, routine.getName())
                                .get();
                        invoker.invokeNamedFunction(DISTINGUISHED_REGISTRATION_METHOD_NAME,
                                new Object[] { new RestFunctionRegistrar() {
                                    @Override
                                    public void register(String jsonSpec) throws Exception {
                                        EndpointMap.this.register(routine.getName().getSchemaName(), routine.getName()
                                                .getTableName(), jsonSpec);
                                    }
                                } });
                    } catch (ExternalRoutineInvocationException e) {
                        // Ignore - expected case when using annotation
                    } catch (Exception e) {
                        // Failed because there is no _register function, or
                        // some other exception
                        // TODO - log and report this somehow
                        e.printStackTrace();
                    }
                }
            }
        }

        void parseAnnotations(final String schemaName, final String procName, final String definition) throws Exception {
            String[] lines = definition.split("\\n");
            String spec = "";
            for (final String s : lines) {
                String line = s.trim();
                if (line.startsWith(COMMENT_ANNOTATION1) || line.startsWith(COMMENT_ANNOTATION2)) {
                    line = line.substring(COMMENT_ANNOTATION1.length()).trim();
                    if (line.regionMatches(true, 0, ENDPOINT, 0, ENDPOINT.length())) {
                        if (!spec.isEmpty()) {
                            registerAnnotation(schemaName, procName, spec);
                        }
                        spec = line.substring(ENDPOINT.length()).trim();
                    } else {
                        if (!spec.isEmpty()) {
                            spec += " " + line;
                        }
                    }
                } else if (!spec.isEmpty()) {
                    registerAnnotation(schemaName, procName, spec);
                    spec = "";
                }
            }
            if (!spec.isEmpty()) {
                registerAnnotation(schemaName, procName, spec);
                spec = "";
            }
        }

        void registerAnnotation(final String schema, final String routine, final String spec) throws Exception {
            if (spec.startsWith("(")) {
                final Tokenizer tokens = new Tokenizer(spec, ", ");
                tokens.grouped = true;
                register(schema, routine, tokens.next(true));
            } else {
                register(schema, routine, spec);
            }
        }

        void register(final String schema, final String routine, final String spec) throws Exception {

            EndpointMetadata em = EndpointMetadata.createEndpointMetadata(schema, routine, spec);
            EndpointAddress ea = new EndpointAddress(em.method, new TableName(schema, em.name));

            synchronized (map) {
                List<EndpointMetadata> list = map.get(ea);
                if (list == null) {
                    list = new LinkedList<EndpointMetadata>();
                    map.put(ea, list);
                }
                list.add(em);
            }
        }
    }

    private EndpointMap getEndpointMap(final Session session) {
        final AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(session);
        return ais.getCachedValue(ENDPOINT_MAP_CACHE_KEY, new CacheValueGenerator<EndpointMap>() {

            @Override
            public EndpointMap valueFor(AkibanInformationSchema ais) {
                EndpointMap em = new EndpointMap(reqs);
                em.populate(ais, session);
                return em;
            }

        });
    }
}
