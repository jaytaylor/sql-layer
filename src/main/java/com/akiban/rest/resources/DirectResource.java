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
import static com.akiban.util.JsonUtils.readTree;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Date;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.codehaus.jackson.JsonNode;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
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
import com.akiban.rest.resources.DirectResource.EndpointMetadata.ParamCache;
import com.akiban.rest.resources.DirectResource.EndpointMetadata.ParamMetadata;
import com.akiban.rest.resources.DirectResource.EndpointMetadata.ParamSourceMetadata;
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
    private final static String PACKAGE = "com.akiban.direct.entity";

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

    private final static String CREATE_PROCEDURE_FORMAT = "CREATE PROCEDURE %s (IN json_in VARCHAR(65535), OUT json_out VARCHAR(65535))"
            + " LANGUAGE %s PARAMETER STYLE json AS $$%s$$";

    private final static String DROP_PROCEDURE_FORMAT = "DROP PROCEDURE %s";

    private final static Charset UTF8 = Charset.forName("UTF8");

    private final ResourceRequirements reqs;

    final Map<EndpointAddress, List<EndpointMetadata>> endpointMap = new HashMap<>();

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
                ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, false);
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
                ClassBuilder helper = new ClassSourceWriter(writer, PACKAGE, false);
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
                ClassBuilder helper = new ClassXRefWriter(writer, PACKAGE);
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
                reqs.restDMLService.callRegistrationProcedure(writer, request, jsonp, procName,
                        new RestFunctionRegistrar() {
                            @Override
                            public void register(String jsonSpec) throws Exception {
                                DirectResource.this.register(procName.getSchemaName(), procName.getTableName(),
                                        jsonSpec);
                            }
                        });
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
                unregister(procName.getSchemaName(), procName.getTableName());
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
            @PathParam("params") final String pathParams,
            @QueryParam(SCHEMA_ARG_NAME) @DefaultValue("") final String schema, @Context final UriInfo uri) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, schema, "GET", procName, pathParams,
                        uri.getQueryParameters(), null, DirectResource.this);
            }
        }).build();
    }

    @POST
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callPost(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams,
            @QueryParam(SCHEMA_ARG_NAME) @DefaultValue("") final String schema, @Context final UriInfo uri,
            final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, schema, "POST", procName, pathParams,
                        uri.getQueryParameters(), content, DirectResource.this);
            }
        }).build();
    }

    @PUT
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callPut(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams,
            @QueryParam(SCHEMA_ARG_NAME) @DefaultValue("") final String schema, @Context final UriInfo uri,
            final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, schema, "PUT", procName, pathParams,
                        uri.getQueryParameters(), content, DirectResource.this);
            }
        }).build();
    }

    @DELETE
    @Path("/call/{proc}{params:(/.*)?}")
    public Response callDelete(@Context final HttpServletRequest request, @PathParam("proc") final String proc,
            @PathParam("params") final String pathParams,
            @QueryParam(SCHEMA_ARG_NAME) @DefaultValue("") final String schema, @Context final UriInfo uri,
            final byte[] content) {
        final TableName procName = ResourceHelper.parseTableName(request, proc);
        ResourceHelper.checkSchemaAccessible(reqs.securityService, request, procName.getSchemaName());

        return RestResponseBuilder.forRequest(request).body(new BodyGenerator() {
            @Override
            public void write(PrintWriter writer) throws Exception {
                reqs.restDMLService.invokeRestEndpoint(writer, request, schema, "DELETE", procName, pathParams,
                        uri.getQueryParameters(), content, DirectResource.this);
            }
        }).build();
    }

    private String text(final JsonNode node, boolean requiredNonEmpty) throws Exception {
        String s = null;
        if (node != null) {
            s = node.asText();
        }
        if (requiredNonEmpty) {
            if (s == null || s.isEmpty()) {
                throw new IllegalArgumentException("Value must be specified");
            }
        }
        return s == null ? "" : s;
    }

    @Override
    public void invokeRestFunction(final PrintWriter writer, JDBCConnection conn, final String method,
            final TableName procName, final String pathParams, final MultivaluedMap<String, String> queryParameters,
            final byte[] content) throws Exception {
        final List<EndpointMetadata> list;
        synchronized (endpointMap) {
            list = endpointMap.get(new EndpointAddress(method, procName));
        }
        EndpointMetadata md = null;
        ParamCache cache = new ParamCache();
        for (final EndpointMetadata candidate : list) {
            if (candidate.pattern != null) {
                Matcher matcher = candidate.getParamPathMatcher(cache, pathParams);
                if (matcher.matches()) {
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
        if (md == null) {
            throw new Exception("no endpoint"); // TODO
        }
        final Object[] args = new Object[md.inParams.length];
        for (int index = 0; index < md.inParams.length; index++) {
            final ParamMetadata pm = md.inParams[index];
            Object v = pm.source.value(pathParams, queryParameters, content, cache);
            args[index] = convertType(pm, v);
        }

        final ScriptInvoker invoker = conn.getRoutineLoader()
                .getScriptInvoker(conn.getSession(), new TableName(conn.getSchema(), md.routineName)).get();
        invoker.invokeNamedFunction(md.function, args);

    }

    private Object convertType(ParamMetadata pm, Object v) throws Exception {

        if (v == null) {
            if (pm.defaultValue != null) {
                return pm.defaultValue;
            } else if (pm.required) {
                throw new IllegalArgumentException("Argument for " + pm + " may not be null");
            } else {
                return null;
            }
        }

        switch (pm.type) {
        case EndpointMetadata.X_TYPE_INT:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isInt()) {
                    return ((JsonNode) v).getIntValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Integer.parseInt((String) v);
            }
        case EndpointMetadata.X_TYPE_LONG:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isLong()) {
                    return ((JsonNode) v).getLongValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Long.parseLong((String) v);
            }
        case EndpointMetadata.X_TYPE_FLOAT:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isFloatingPointNumber()) {
                    return ((JsonNode) v).getNumberValue().floatValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Float.parseFloat((String) v);
            }
        case EndpointMetadata.X_TYPE_DOUBLE:
            if (v instanceof JsonNode) {
                if (((JsonNode) v).isFloatingPointNumber()) {
                    return ((JsonNode) v).getNumberValue().doubleValue();
                } else {
                    break;
                }
            } else {
                assert v instanceof String;
                return Double.parseDouble((String) v);
            }
        case EndpointMetadata.X_TYPE_STRING:
            return asString(pm, v);
        case EndpointMetadata.X_TYPE_DATE:
            return asDate(pm, v);
        case EndpointMetadata.X_TYPE_BYTEARRAY:
            assert v instanceof byte[];
            return v;
        case EndpointMetadata.X_TYPE_JSON:
            String json = asString(pm, v);
            return readTree(json);
        default:
        }
        throw new IllegalArgumentException("Type specified by " + pm + " is not supported");
    }

    private String asString(ParamMetadata pm, Object v) {
        if (v instanceof JsonNode) {
            if (((JsonNode) v).isTextual()) {
                return ((JsonNode) v).getTextValue();
            } else {
                throw new IllegalArgumentException("JsonNode " + v + " is not textual");
            }
        } else {
            assert v instanceof String;
            return (String) v;
        }
    }

    private Date asDate(ParamMetadata pm, Object v) throws ParseException {
        String s = asString(pm, v);
        if ("today".equalsIgnoreCase(s)) {
            return new Date(System.currentTimeMillis());
        }
        return Date.valueOf(s);
    }

    /*
     * ---------------------------------------------------------------------
     */

    public void register(final String schema, final String routine, final String spec) throws Exception {
        JsonNode tree = readTree(spec);
        final String function = text(tree.get("function"), true);
        final String method = text(tree.get("method"), true);
        final String name = text(tree.get("name"), true);
        final String pathParams = text(tree.get("pathParams"), false);
        final String jsonParams = text(tree.get("jsonParams"), false);
        final String queryParams = text(tree.get("queryParams"), false);
        final String contentParam = text(tree.get("contentParam"), false);
        List<String> inParams = tree.findValuesAsText("in");
        String outParam = text(tree.get("out"), false);
        register(schema, routine, function, method, name, pathParams, jsonParams, queryParams, contentParam, inParams,
                outParam);
    }
    
    public void unregister(final String schemaName, final String routineName) {
        synchronized(endpointMap) {
            for (Iterator<Map.Entry<EndpointAddress, List<EndpointMetadata>>> entryIter = endpointMap.entrySet().iterator(); entryIter.hasNext();) {
                final Map.Entry<EndpointAddress, List<EndpointMetadata>> entry = entryIter.next();
                final EndpointAddress ea = entry.getKey();
                final List<EndpointMetadata> list = entry.getValue();
                for (Iterator<EndpointMetadata> mdIter = list.iterator(); mdIter.hasNext();) {
                    EndpointMetadata md = mdIter.next();
                    if (routineName.equals(md.routineName) && schemaName.equals(ea.schema)) {
                        mdIter.remove();
                    }
                }
                if (list.isEmpty()) {
                    entryIter.remove();
                }
            }
        }
    }

    void register(final String schema, final String routine, final String function, final String method,
            final String name, final String pathParams, final String jsonParams, final String queryParams,
            final String contentParam, final List<String> inParams, final String outParam) throws Exception {

        EndpointMetadata md = createEndpointMetadata(function, pathParams, jsonParams, queryParams, contentParam,
                inParams, outParam);
        md.routineName = routine;
        if (!("GET".equals(method)) && !("POST".equals(method)) && !("PUT".equals(method))
                && !("DELETE".equals(method))) {
            throw new IllegalArgumentException("Method must be GET, POST, PUT or DELETE");
        }
        EndpointAddress ea = new EndpointAddress(method, new TableName(schema, name));

        synchronized (endpointMap) {
            List<EndpointMetadata> list = endpointMap.get(ea);
            if (list == null) {
                list = new LinkedList<EndpointMetadata>();
                endpointMap.put(ea, list);
            }
            list.add(md);
        }
    }

    EndpointMetadata createEndpointMetadata(final String function, final String pathParams, final String jsonParams,
            final String queryParams, final String contentParam, final List<String> inParams, final String outParam)
            throws Exception {
        Map<String, ParamSourceMetadata> paramMap = new HashMap<String, ParamSourceMetadata>();
        String pathParamsPattern = null;

        if (pathParams != null) {
            pathParamsPattern = registerPathParams(pathParams, paramMap);
        }
        if (jsonParams != null) {
            registerJsonParams(jsonParams, paramMap);
        }
        if (queryParams != null) {
            registerQueryParams(queryParams, paramMap);
        }
        if (contentParam != null) {
            registerContentParam(contentParam, paramMap);
        }

        EndpointMetadata md = new EndpointMetadata();
        if (pathParamsPattern != null) {
            md.pattern = Pattern.compile(pathParamsPattern);
        }
        md.function = function;
        md.inParams = new ParamMetadata[inParams.size()];
        for (int index = 0; index < md.inParams.length; index++) {
            final ParamMetadata pm = new ParamMetadata();
            final Tokenizer tokens = new Tokenizer(inParams.get(index), " ");
            final String paramName = tokens.next(true);

            final ParamSourceMetadata psm = paramMap.get(paramName);
            if (psm == null) {
                throw new IllegalArgumentException("In param does not specify source: " + inParams.get(index));
            }
            String type = tokens.next(true);
            if (!EndpointMetadata.X_TYPES.contains(type)) {
                throw new IllegalArgumentException("Unknown parameter type " + type);
            }
            pm.source = psm;
            pm.name = paramName;
            pm.type = type;
            String qualifier;
            while (!(qualifier = tokens.next(false)).isEmpty()) {
                if ("required".equals(qualifier)) {
                    pm.required = true;
                } else if ("default".equals(qualifier)) {
                    String v = tokens.next(true);
                    pm.defaultValue = convertType(pm, v);
                }
            }
            md.inParams[index] = pm;
        }
        if (outParam != null) {
            final Tokenizer tokens = new Tokenizer(outParam, " ");
            final ParamMetadata pm = new ParamMetadata();
            String type = tokens.next(true);
            if (!EndpointMetadata.X_TYPES.contains(type)) {
                throw new IllegalArgumentException("Unknown parameter type " + type);
            }
            pm.type = type;
            String qualifier;
            while (!(qualifier = tokens.next(false)).isEmpty()) {
                if ("default".equals(qualifier)) {
                    String v = tokens.next(true);
                    pm.defaultValue = convertType(pm, v);
                }
            }
            md.outParam = pm;
        }
        return md;
    }

    private void registerContentParam(final String contentParam, Map<String, ParamSourceMetadata> paramMap) {
        String n = contentParam.trim();
        for (int i = 0; i < n.length(); i++) {
            if (!Character.isLetter(n.charAt(i))) {
                throw new IllegalArgumentException("Invalid contentParam specification: " + contentParam);
            }
        }
        ParamSourceMetadata pm = new EndpointMetadata.ParamSourceContentString();
        Object other = paramMap.put(n, pm);
        if (other != null) {
            throw new IllegalArgumentException("Multiple parameters named " + n);
        }
    }

    private void registerQueryParams(final String queryParams, Map<String, ParamSourceMetadata> paramMap) {
        Tokenizer tokens = new Tokenizer(queryParams, ", ");
        String name;
        while (!(name = tokens.nextName(false)).isEmpty()) {
            ParamSourceMetadata pm = new EndpointMetadata.ParamSourceQueryParam(name);
            Object other = paramMap.put(name, pm);
            if (other != null) {
                throw new IllegalArgumentException("Multiple parameters named " + name);
            }
        }
    }

    private void registerJsonParams(final String jsonParams, Map<String, ParamSourceMetadata> paramMap) {
        Tokenizer tokens = new Tokenizer(jsonParams, ", ");
        String name;
        while (!(name = tokens.nextName(false)).isEmpty()) {
            ParamSourceMetadata pm = new EndpointMetadata.ParamSourceJson(name);
            Object other = paramMap.put(name, pm);
            if (other != null) {
                throw new IllegalArgumentException("Multiple parameters named " + name);
            }
        }
    }

    private String registerPathParams(final String pathParams, Map<String, ParamSourceMetadata> paramMap) {
        StringBuilder pattern = new StringBuilder();
        int startParamName = -1;
        int paramCount = 0;
        for (int i = 0; i < pathParams.length(); i++) {
            char c = pathParams.charAt(i);
            if (startParamName > -1) {
                if (c == '>' && i - startParamName > 1) {
                    String paramName = pathParams.substring(startParamName, i);
                    ParamSourceMetadata pm = new EndpointMetadata.ParamSourcePath(++paramCount);
                    Object other = paramMap.put(paramName, pm);
                    if (other != null) {
                        throw new IllegalArgumentException("Multiple parameters named " + paramName);
                    }
                    startParamName = -1;
                    pattern.append("([^/]*)");
                } else if (!Character.isLetter(c)) {
                    throw new IllegalArgumentException("Invalid pathParams specification: " + pathParams);
                }
            } else {
                if (c == '<') {
                    startParamName = i + 1;
                } else {
                    if ("\\.*+?()[]".indexOf(c) > -1) {
                        pattern.append('\\');
                    }
                    pattern.append(c);
                }
            }
        }
        return pattern.toString();
    }

    /**
     * Meta-data for a registered Akiban Direct REST end-point.
     * 
     * method:"GET", path:"totalCompensation/<empid>?start_date=<start>",
     * in:["empid as int not null", "percent as float not null"],
     * out:["result as json"], language:"js"
     * 
     * {method:"GET", name="totalCompensation", pathParams:"/<empid>",
     * queryParams:"start_date", in:["empid int required",
     * "start_date date default '20010101'"], out:"int"}
     * 
     * {method:"POST", name="changeSalary",
     * jsonParams:"empid, percent, start_date" in:["empid int required",
     * "percent float required", "start_date date 'TODAY'"], out:"float"}
     * 
     * {method:"POST", name="postOrder", contentParam:"content",
     * in:["content json required"], out:"json"}
     * 
     * Type names:
     * 
     * string int long float double Date Bytearray Json
     * 
     * In parameters: paramName type [required | default 'strval']
     * 
     * Out parameter: type [default 'strval']
     * 
     */
    static class EndpointMetadata {

        private final static String X_TYPE_INT = "int";
        private final static String X_TYPE_LONG = "long";
        private final static String X_TYPE_FLOAT = "float";
        private final static String X_TYPE_DOUBLE = "double";
        private final static String X_TYPE_STRING = "String";
        private final static String X_TYPE_DATE = "Date";
        private final static String X_TYPE_BYTEARRAY = "Bytearray";
        private final static String X_TYPE_JSON = "Json";

        private final static List<String> X_TYPES = Arrays.asList(new String[] { X_TYPE_INT, X_TYPE_LONG, X_TYPE_FLOAT,
                X_TYPE_DOUBLE, X_TYPE_STRING, X_TYPE_DATE, X_TYPE_BYTEARRAY, X_TYPE_JSON });

        /**
         * Name of the Routine that contains the function definition
         */
        String routineName;
        /**
         * Name of the function to call in the script
         */
        String function;

        Pattern pattern;

        /**
         * Parameter meta-data for the return value (specifies type and optional
         * default value)
         */
        ParamMetadata outParam;

        /**
         * Parameter meta-data for input parameters
         */
        ParamMetadata[] inParams;

        @Override
        public String toString() {
            String s = "routine=" + routineName;
            s += ",function=" + function;
            if (pattern != null) {
                s += ",pathParamPattern=" + pattern;
            }
            if (outParam != null) {
                s += ",out:" + outParam;
            }
            s += ",in:" + Arrays.asList(inParams);
            return s;
        }

        void setPathParameterPattern(final String s) {
            pattern = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
        }

        Matcher getParamPathMatcher(final ParamCache cache, final String pathParamString) {
            if (cache.matcher == null) {
                cache.matcher = pattern.matcher(pathParamString);
            }
            return cache.matcher;
        }

        /**
         * Meta-data for one REST call parameter.
         */
        static class ParamMetadata {
            String name;
            String type;
            boolean required;
            Object defaultValue;
            ParamSourceMetadata source;

            @Override
            public String toString() {
                String s = "(" + name + " " + type;
                if (required) {
                    s += " required";
                }
                if (defaultValue != null) {
                    s += " default=\'" + defaultValue + "\'";
                }
                s += " " + source + ")";
                return s;
            }
        }

        /**
         * Cache to hold partial result, e.b. the JsonNode of the root of a tree
         * created by parsing the request body as JSON.
         */
        static class ParamCache {
            Matcher matcher;
            JsonNode tree;
        }

        abstract static class ParamSourceMetadata {
            abstract Object value(final String pathParams, final MultivaluedMap<String, String> queryParams,
                    Object content, ParamCache cache);
        }

        /**
         * Meta-data for a parameter intended to be filled from the query
         * parameters of the REST call.
         */
        static class ParamSourceQueryParam extends ParamSourceMetadata {

            final String paramName;

            ParamSourceQueryParam(final String paramName) {
                this.paramName = paramName;
            }

            Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                    ParamCache cache) {
                List<String> values = queryParams.get(paramName);
                if (values != null) {
                    if (values.size() == 1) {
                        return values.get(0);
                    } else {
                        return values;
                    }
                } else {
                    return null;
                }
            }

            @Override
            public String toString() {
                return "(QueryParam:" + paramName + ")";
            }
        }

        /**
         * Meta-data for a parameter intended to be filled from the URI of the
         * REST call. The procedure call end-point aggregates all text of the
         * URI following procedure name into a string; this class defines a
         * Pattern for parsing out an field from that text using a supplied
         * RegEx pattern. The pattern is case-insensitive.
         */
        static class ParamSourcePath extends ParamSourceMetadata {
            final int matchingGroup;

            ParamSourcePath(final int matchingGroup) {
                this.matchingGroup = matchingGroup;
            }

            Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                    ParamCache cache) {
                if (cache.matcher.matches()) {
                    return cache.matcher.group(matchingGroup);
                } else {
                    return null;
                }
            }

            @Override
            public String toString() {
                return "(PathParam:" + matchingGroup + ")";
            }
        }

        /**
         * Meta-data for a parameter that conveys the byte array supplied in the
         * request body of the REST call. This is intended to support a rest
         * call that receives a binary payload.
         */
        static class ParamSourceContentBytes extends ParamSourceMetadata {
            Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                    ParamCache cache) {
                assert content instanceof byte[];
                return content;
            }

            @Override
            public String toString() {
                return "(ContentBytes)";
            }
        }

        /**
         * Meta-data for a parameter that conveys the byte array supplied in the
         * request body of the REST call. This is intended to support a rest
         * call that receives a text-valued payload.
         */
        static class ParamSourceContentString extends ParamSourceMetadata {
            Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                    ParamCache cache) {
                assert content instanceof String;
                return new String((byte[]) content, UTF8);
            }

            @Override
            public String toString() {
                return "(ContentString)";
            }
        }

        static class ParamSourceJson extends ParamSourceMetadata {

            final String paramName;

            ParamSourceJson(final String paramName) {
                this.paramName = paramName;
            }

            Object value(final String pathParams, final MultivaluedMap<String, String> queryParams, Object content,
                    ParamCache cache) {
                if (cache.tree == null) {
                    assert content instanceof String;
                    try {
                        cache.tree = readTree((String) content);
                    } catch (IOException e) {
                        return null;
                    }
                }
                return cache.tree.get(paramName);
            }

            @Override
            public String toString() {
                return "(JsonParam:" + paramName + ")";
            }
        }

    }

    private static class EndpointAddress implements Comparable<EndpointAddress> {
        /**
         * One of GET, POST, PUT or DELETE
         */
        private final String method;

        /**
         * Schema name
         */
        private final String schema;
        /**
         * Name of the end-point
         */
        private final String name;

        EndpointAddress(final String method, TableName procName) {
            this.method = method;
            this.schema = procName.getSchemaName();
            this.name = procName.getTableName();
        }

        /**
         * Sorts by schema, method, procedure name.
         */
        public int compareTo(EndpointAddress other) {
            int c = schema.compareTo(other.schema);
            if (c == 0) {
                c = method.compareTo(other.method);
            }
            if (c == 0) {
                c = name.compareTo(other.name);
            }
            return c;
        }

        @Override
        public int hashCode() {
            return name.hashCode() ^ schema.hashCode() ^ method.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            EndpointAddress ea = (EndpointAddress) other;
            return name.equals(ea.name) && method.equals(ea.method);
        }

    }

    private static class Tokenizer {
        final String source;
        final StringBuilder result = new StringBuilder();
        final String delimiters;
        int index = 0;

        Tokenizer(String source, String delimiters) {
            this.source = source;
            this.delimiters = delimiters;
        }

        String next(boolean required) {
            result.setLength(0);
            boolean quoted = false;
            boolean literal = false;
            boolean first = true;
            for (; index < source.length(); index++) {
                char c = source.charAt(index);
                if (quoted) {
                    quoted = false;
                    result.append(c);
                } else if (c == '\\') {
                    quoted = true;
                } else if (c == '\'') {
                    if (first) {
                        literal = true;
                    } else if (literal) {
                        literal = false;
                        index++;
                        break;
                    }
                } else if (!literal && delimiters.indexOf(c) >= 0) {
                    index++;
                    break;
                } else {
                    result.append(c);
                }
                first = false;
            }
            eatExtraSpaces();
            if (required && result.length() == 0) {
                throw new IllegalArgumentException("Token missing: " + source);
            }
            return result.toString();
        }

        String nextName(final boolean required) {
            result.setLength(0);
            boolean first = true;
            for (; index < source.length(); index++) {
                char c = source.charAt(index);
                if (delimiters.indexOf(c) >= 0) {
                    index++;
                    break;
                }
                if (!Character.isLetterOrDigit(c) || (first && !Character.isLetter(c))) {
                    throw new IllegalArgumentException("Invalid character in name: " + source);
                }
                result.append(c);
                first = false;
            }
            eatExtraSpaces();
            if (required && result.length() == 0) {
                throw new IllegalArgumentException("Token missing: " + source);
            }
            return result.toString();
        }

        private void eatExtraSpaces() {
            while (index < source.length() && source.charAt(index) == ' ') {
                index++;
            }
        }
    }

}
