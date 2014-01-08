/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.service.restdml;

import static com.foundationdb.rest.resources.ResourceHelper.checkSchemaAccessible;
import static com.foundationdb.rest.resources.ResourceHelper.checkTableAccessible;
import static com.foundationdb.util.JsonUtils.createJsonGenerator;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Routine.CallingConvention;
import com.foundationdb.ais.model.Schema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.direct.Direct;
import com.foundationdb.rest.RestFunctionRegistrar;
import com.foundationdb.rest.resources.ResourceHelper;
import com.foundationdb.server.error.DirectEndpointNotFoundException;
import com.foundationdb.server.error.DirectTransactionFailedException;
import com.foundationdb.server.error.ExternalRoutineInvocationException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.ScriptLibraryRegistrationException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.restdml.EndpointMetadata.EndpointAddress;
import com.foundationdb.server.service.restdml.EndpointMetadata.ParamCache;
import com.foundationdb.server.service.restdml.EndpointMetadata.ParamMetadata;
import com.foundationdb.server.service.routines.RoutineLoader;
import com.foundationdb.server.service.routines.ScriptLibrary;
import com.foundationdb.server.service.routines.ScriptPool;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.JDBCConnection;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.inject.Inject;
import com.persistit.exception.RollbackException;

public class DirectServiceImpl implements Service, DirectService {

    private static final Logger LOG = LoggerFactory.getLogger(DirectService.class.getName());

    private final static String LANGUAGE = "language";
    private final static String FUNCTIONS = "functions";

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

    private final static int TRANSACTION_RETRY_COUNT = 3;

    private final static String DISTINGUISHED_REGISTRATION_METHOD_NAME = "_register";

    private final static String CREATE_PROCEDURE_FORMAT = "CREATE OR REPLACE PROCEDURE \"%s\".\"%s\" ()"
            + " LANGUAGE %s PARAMETER STYLE LIBRARY AS $$%s$$";

    private final static String DROP_PROCEDURE_FORMAT = "DROP PROCEDURE \"%s\".\"%s\"";

    private final static Object ENDPOINT_MAP_CACHE_KEY = new Object();

    private final SecurityService securityService;
    private final DXLService dxlService;
    private final EmbeddedJDBCService jdbcService;
    private final RoutineLoader routineLoader;
    
    
    @Inject
    public DirectServiceImpl(SecurityService securityService, DXLService dxlService, EmbeddedJDBCService jdbcService,
            RoutineLoader routineLoader) {
        this.securityService = securityService;
        this.dxlService = dxlService;
        this.jdbcService = jdbcService;
        this.routineLoader = routineLoader;
    }

    /* Service */

    @Override
    public void start() {
        // None
    }

    @Override
    public void stop() {
        // None
    }

    @Override
    public void crash() {
        // None
    }

    /* DirectService */

    @Override
    public void installLibrary(final PrintWriter writer, final HttpServletRequest request, final String module,
            final String definition, final String language) throws Exception {
        try (final JDBCConnection conn = jdbcConnection(request); final Statement statement = conn.createStatement()) {
            final TableName procName = ResourceHelper.parseTableName(request, module);
            /*
             * TODO - once it becomes possible to execute DDL statements within
             * the scope of an existing transaction, the following should be
             * changed. Right now we perform consecutive DDL operations (create
             * the procedure and then drop it again if there's an error in the
             * _register function) in separate transactions, and in both cases
             * we construct a new AIS.
             */
            final String create = String.format(CREATE_PROCEDURE_FORMAT, procName.getSchemaName(),
                    procName.getTableName(), language, definition);
            statement.execute(create);
            try {
                // Note: the side effect of the following call is to register
                // all functions in the new AIS
                final EndpointMap endpointMap = getEndpointMap(conn.getSession());
                reportLibraryFunctionCount(createJsonGenerator(writer), procName, endpointMap);
            } catch (RegistrationException e) {
                try {
                    final String drop = String.format(DROP_PROCEDURE_FORMAT, procName.getSchemaName(),
                            procName.getTableName(), language, definition);
                    statement.execute(drop);
                } catch (Exception e2) {
                    LOG.error("Unable to remove invalid library " + module, e2);
                }
                throw new ScriptLibraryRegistrationException(e);
            }
        }
    }

    @Override
    public void removeLibrary(final PrintWriter writer, final HttpServletRequest request, final String module)
            throws Exception {
        try (final JDBCConnection conn = jdbcConnection(request); final Statement statement = conn.createStatement()) {
            final TableName procName = ResourceHelper.parseTableName(request, module);
            final String drop = String.format(DROP_PROCEDURE_FORMAT, procName.getSchemaName(), procName.getTableName());
            statement.execute(drop);
            try {
                // Note: the side effect of the following call is to register
                // all functions in the new AIS
                final EndpointMap endpointMap = getEndpointMap(conn.getSession());
                reportLibraryFunctionCount(createJsonGenerator(writer), procName, endpointMap);
            } catch (RegistrationException e) {
                throw new ScriptLibraryRegistrationException(e);
            }
        }
    }

    @Override
    public void reportStoredProcedures(final PrintWriter writer, final HttpServletRequest request,
            final String suppliedSchemaName, final String module, final Session session, boolean functionsOnly)
            throws Exception {
        final String schemaName = suppliedSchemaName.isEmpty() ? ResourceHelper.getSchema(request) : suppliedSchemaName;

        if (module.isEmpty()) {
            checkSchemaAccessible(securityService, request, schemaName);
        } else {
            checkTableAccessible(securityService, request, new TableName(schemaName, module));
        }
        JsonGenerator json = createJsonGenerator(writer);
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        EndpointMap endpointMap = null;

        try {
            if (functionsOnly) {
                endpointMap = getEndpointMap(session);
            }
        } catch (RegistrationException e) {
            throw new ScriptLibraryRegistrationException(e);
        }

        if (module.isEmpty()) {
            // Get all routines in the schema.
            json.writeStartObject();
            {
                Schema schemaAIS = ais.getSchema(schemaName);
                if (schemaAIS != null) {
                    for (Map.Entry<String, Routine> routineEntry : schemaAIS.getRoutines().entrySet()) {
                        json.writeFieldName(routineEntry.getKey());
                        if (functionsOnly) {
                            reportLibraryFunctionMetadata(json, new TableName(schemaName, routineEntry.getKey()),
                                    endpointMap);
                        } else {
                            reportStoredProcedureDetails(json, routineEntry.getValue());
                        }
                    }
                }
            }
            json.writeEndObject();
        } else {
            // Get just the one routine.
            Routine routine = ais.getRoutine(schemaName, module);
            if (routine == null) {
                throw new NoSuchRoutineException(schemaName, module);
            }
            if (functionsOnly) {
                reportLibraryFunctionCount(json, new TableName(schemaName, module), endpointMap);
            } else {
                reportStoredProcedureDetails(json, routine);
            }
        }
        json.flush();
    }

    private void reportLibraryFunctionCount(final JsonGenerator json, final TableName module,
            final EndpointMap endpointMap) throws Exception {
        int count = 0;
        for (final Map.Entry<EndpointAddress, List<EndpointMetadata>> entry : endpointMap.getMap().entrySet()) {
            count += entry.getValue().size();
        }
        json.writeStartObject();
        json.writeNumberField(FUNCTIONS, count);
        json.writeEndObject();
        json.flush();
    }

    private void reportLibraryFunctionMetadata(final JsonGenerator json, final TableName module,
            final EndpointMap endpointMap) throws Exception {
        json.writeStartObject();
        json.writeArrayFieldStart(FUNCTIONS);
        {
            for (final Map.Entry<EndpointAddress, List<EndpointMetadata>> entry : endpointMap.getMap().entrySet()) {
                for (final EndpointMetadata em : entry.getValue()) {
                    json.writeString(em.toString());
                }
            }
        }
        json.writeEndArray();
        json.writeEndObject();
        json.flush();
    }

    private void reportStoredProcedureDetails(JsonGenerator json, Routine routine) throws IOException {
        json.writeStartObject();
        {
            json.writeStringField(LANGUAGE, routine.getLanguage());
            json.writeStringField(CALLING_CONVENTION, routine.getCallingConvention().name());
            json.writeNumberField(MAX_DYNAMIC_RESULT_SETS, routine.getDynamicResultSets());
            if (routine.getDefinition() != null) {
                json.writeStringField(DEFINITION, routine.getDefinition().replace("\r", ""));
            }
            reportLibraryDetailsParams(PARAMETERS_IN, routine.getParameters(), Parameter.Direction.IN, json);
            reportLibraryDetailsParams(PARAMETERS_OUT, routine.getParameters(), Parameter.Direction.OUT, json);
        }
        json.writeEndObject();
        json.flush();
    }

    private void reportLibraryDetailsParams(String label, List<Parameter> parameters, Parameter.Direction direction,
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
                        TInstance type = param.getType();
                        TClass tClass = param.getType().typeClass();
                        json.writeStringField(TYPE, tClass.name().unqualifiedName());
                        json.writeObjectFieldStart(TYPE_OPTIONS);
                        {
                            for (Attribute attr : tClass.attributes())
                                json.writeObjectField(attr.name().toLowerCase(), type.attributeToObject(attr));
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

    public DirectInvocation prepareRestInvocation(final String method, final TableName procName,
            final String pathParams, final MultivaluedMap<String, String> queryParameters, final byte[] content,
            final HttpServletRequest request) throws Exception {

        JDBCConnection conn = jdbcConnection(request, procName.getSchemaName());
        ParamCache cache = new ParamCache();
        final EndpointMap endpointMap = getEndpointMap(conn.getSession());
        final List<EndpointMetadata> list;
        synchronized (endpointMap) {
            list = endpointMap.getMap().get(new EndpointAddress(method, procName));
        }
        EndpointMetadata em = selectEndpoint(list, pathParams, request.getContentType(), cache);
        if (em == null) {
            throw new DirectEndpointNotFoundException(method, request.getRequestURI());
        }
        final Object[] args = createArgsArray(pathParams, queryParameters, content, cache, em);
        return new DirectInvocation(conn, procName, em, args);
    }

    public void invokeRestEndpoint(final PrintWriter writer, final HttpServletRequest request, final String method,
            final DirectInvocation in) throws Exception {
        LOG.debug("Invoking {} {}", method, request.getRequestURI());

        JDBCConnection conn = in.getConnection();
        conn.setAutoCommit(false);
        boolean completed = false;
        int repeat = TRANSACTION_RETRY_COUNT;

        while (--repeat >= 0) {
            try {
                Direct.enter(in.getProcName().getSchemaName(), dxlService.ddlFunctions().getAIS(conn.getSession()));
                Direct.getContext().setConnection(conn);
                conn.beginTransaction();
                invokeRestFunction(writer, in);
                conn.commitTransaction();
                completed = true;
                return;
            } catch (RollbackException e) {
                if (repeat == 0) {
                    LOG.error("Transaction failed " + TRANSACTION_RETRY_COUNT + " times: " + request.getRequestURI());
                    throw new DirectTransactionFailedException(method, request.getRequestURI());
                }
            } catch (RegistrationException e) {
                throw new ScriptLibraryRegistrationException(e);
            } finally {
                try {
                    if (!completed) {
                        conn.rollbackTransaction();
                    }
                } finally {
                    Direct.leave();
                }
            }
        }
    }


    private void invokeRestFunction(final PrintWriter writer, DirectInvocation in) throws Exception {
        EndpointMetadata em = in.getEndpointMetadata();
        Object[] args = in.getArgs();

        final ScriptPool<ScriptLibrary> libraryPool = in
                .getConnection()
                .getRoutineLoader()
                .getScriptLibrary(in.getConnection().getSession(),
                        new TableName(in.getProcName().getSchemaName(), em.routineName));
        final ScriptLibrary library = libraryPool.get();

        boolean success = false;
        Object result;

        LOG.debug("Endpoint {}", in.getEndpointMetadata());
        try {
            result = library.invoke(em.function, args);
            success = true;
        } finally {
            libraryPool.put(library, success);
        }

        switch (em.outParam.type) {

        case EndpointMetadata.X_TYPE_STRING:
            if (result != null) {
                writer.write(result.toString());
            } else if (em.outParam.defaultValue != null) {
                writer.write(em.outParam.defaultValue.toString());
            }
            break;
            
        case EndpointMetadata.X_TYPE_JSON:
            JsonGenerator json = createJsonGenerator(writer);
            if (result != null) {
                json.writeObject(result);
            } else if (em.outParam.defaultValue != null) {
                json.writeObject(em.outParam.defaultValue);
            }
            json.flush();
            break;

        case EndpointMetadata.X_TYPE_VOID:
            break;

        case EndpointMetadata.X_TYPE_BYTEARRAY:
            /*
             * intentionally falls through TODO: support X_TYPE_BYTEARRAY
             */

        default:
            assert false : "Invalid output type";
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
            final String requestType, ParamCache cache) {
        EndpointMetadata em = null;
        if (list != null) {
            for (final EndpointMetadata candidate : list) {
                if (requestType != null && candidate.expectedContentType != null
                        && !requestType.startsWith(candidate.expectedContentType)) {
                    continue;
                }
                if (candidate.pattern != null) {
                    Matcher matcher = candidate.getParamPathMatcher(cache, pathParams);
                    if (matcher.matches()) {
                        em = candidate;
                        break;
                    }
                } else {
                    if (pathParams == null || pathParams.isEmpty()) {
                        em = candidate;
                        break;
                    }
                }
            }
        }
        return em;
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
     * @param em
     *            The <code>EndpointMetadata</code> instance selected by
     *            {@link #selectEndpoint(List, String, String, MediaType[], ParamCache)}
     * @return the argument array
     * @throws Exception
     */
    Object[] createArgsArray(final String pathParams, final MultivaluedMap<String, String> queryParameters,
            final byte[] content, ParamCache cache, EndpointMetadata em) throws Exception {
        final Object[] args = new Object[em.inParams.length];
        for (int index = 0; index < em.inParams.length; index++) {
            final ParamMetadata pm = em.inParams[index];
            Object v = pm.source.value(pathParams, queryParameters, content, cache);
            args[index] = EndpointMetadata.convertType(pm, v);
        }
        return args;
    }

    static class EndpointMap {

        final RoutineLoader routineLoader;
        final Map<EndpointAddress, List<EndpointMetadata>> map = new HashMap<EndpointAddress, List<EndpointMetadata>>();

        EndpointMap(final RoutineLoader routineLoader) {
            this.routineLoader = routineLoader;
        }

        Map<EndpointAddress, List<EndpointMetadata>> getMap() {
            return map;
        }

        void populate(final AkibanInformationSchema ais, final Session session) throws RegistrationException {
            for (final Routine routine : ais.getRoutines().values()) {
                if (routine.getCallingConvention().equals(CallingConvention.SCRIPT_LIBRARY)
                        && routine.getDynamicResultSets() == 0 && routine.getParameters().isEmpty()) {
                    final ScriptPool<ScriptLibrary> libraryPool = routineLoader.getScriptLibrary(session,
                            routine.getName());
                    final ScriptLibrary library = libraryPool.get();
                    boolean success = false;
                    try {
                        library.invoke(DISTINGUISHED_REGISTRATION_METHOD_NAME,
                                new Object[] { new RestFunctionRegistrar() {
                                    @Override
                                    public void register(String specification) throws Exception {
                                        LOG.debug("Registering endpoint in routine {}: {}", routine.getName(),
                                                specification);
                                        EndpointMap.this.register(routine.getName().getSchemaName(), routine.getName()
                                                .getTableName(), specification);
                                    }
                                } });
                        success = true;
                    } catch (ExternalRoutineInvocationException e) {
                        if (e.getCause() instanceof NoSuchMethodException) {
                            LOG.warn("Script library " + routine.getName() + " has no _register function");
                            success = true;
                            continue;
                        }
                        Throwable previous = e;
                        Throwable current;
                        while ((current = previous.getCause()) != null && current != previous) {
                            if (current instanceof RegistrationException) {
                                throw (RegistrationException) current;
                            }
                            previous = current;
                        }
                        throw e;
                    } catch (RegistrationException e) {
                        LOG.warn("Endpoint registration failure {} in routine {}", e, routine.getName());
                        throw e;
                    } catch (Exception e) {
                        LOG.warn("Endpoint registration failure {} in routine {}", e, routine.getName());
                        throw new RegistrationException(e);
                    } finally {
                        libraryPool.put(library, success);
                    }
                }
            }
        }

        void register(final String schema, final String routine, final String spec) throws Exception {

            try {
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
            } catch (Exception e) {
                String msg = IllegalArgumentException.class.equals(e.getClass()) ? e.getMessage() : e.toString();
                throw new RegistrationException("Invalid function specification: " + spec + " - " + msg, e);
            }
        }
    }

    private EndpointMap getEndpointMap(final Session session) {
        final AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        return ais.getCachedValue(ENDPOINT_MAP_CACHE_KEY, new CacheValueGenerator<EndpointMap>() {

            @Override
            public EndpointMap valueFor(AkibanInformationSchema ais) {
                EndpointMap em = new EndpointMap(routineLoader);
                em.populate(ais, session);
                return em;
            }

        });
    }

    private JDBCConnection jdbcConnection(HttpServletRequest request) throws SQLException {
        return (JDBCConnection) jdbcService.newConnection(new Properties(), request.getUserPrincipal());
    }

    private JDBCConnection jdbcConnection(HttpServletRequest request, String schemaName) throws SQLException {
        // TODO: This is to make up for test situations where the
        // request is not authenticated.
        Properties properties = new Properties();
        if ((request.getUserPrincipal() == null) && (schemaName != null)) {
            properties.put("user", schemaName);
        }
        return (JDBCConnection) jdbcService.newConnection(properties, request.getUserPrincipal());
    }

    /**
     * Private unchecked wrapper to communicate errors in function
     * specifications
     * 
     */
    @SuppressWarnings("serial")
    private static class RegistrationException extends RuntimeException {

        RegistrationException(final Throwable t) {
            super(t);
        }

        RegistrationException(final String msg, final Throwable t) {
            super(msg, t);
        }
    }

}
