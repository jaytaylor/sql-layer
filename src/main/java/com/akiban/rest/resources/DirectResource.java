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
import static com.akiban.rest.resources.ResourceHelper.checkSchemaAccessible;
import static com.akiban.rest.resources.ResourceHelper.checkTableAccessible;
import static com.akiban.util.JsonUtils.createJsonGenerator;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.direct.ClassBuilder;
import com.akiban.direct.ClassSourceWriter;
import com.akiban.direct.ClassXRefWriter;
import com.akiban.rest.ResourceRequirements;
import com.akiban.rest.RestResponseBuilder;
import com.akiban.rest.RestResponseBuilder.BodyGenerator;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import org.codehaus.jackson.JsonGenerator;

/**
 * Easy access to the server version
 */
@Path("/direct")
public class DirectResource {

    private final static String TABLE_ARG_NAME = "table";
    private final static String MODULE_ARG_NAME = "module";
    private final static String SCHEMA_ARG_NAME = "schema";
    private final static String LANGUAGE = "language";
    private final static String PACKAGE = "com.akiban.direct.entity";

    private final static String CREATE_PROCEDURE_FORMAT = "CREATE PROCEDURE %s (IN json_in VARCHAR(65535), OUT json_out VARCHAR(65535))"
            + " LANGUAGE %s PARAMETER STYLE json AS $$%s$$";

    private final static String DROP_PROCEDURE_FORMAT = "DROP PROCEDURE %s";

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
     * <li>a methodName with parentheses also includes formal parameter names (which may be convenient for code-completion</li>
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
                try (Session session
                             = reqs.sessionService.createSession();
                     TransactionService.CloseableTransaction txn
                             = reqs.transactionService.beginCloseableTransaction(session)) {
                    JsonGenerator json = createJsonGenerator(writer);
                    AkibanInformationSchema ais = reqs.dxlService.ddlFunctions().getAIS(session);

                    if (procName.isEmpty()) {
                        // Get all routines in the schema.
                        // First, filter and sort them by name. This makes for deterministic tests, as well as easier
                        // viewing in raw form (if anyone cares about that).
                        TreeMap<String, Routine> routinesByName = new TreeMap<>();
                        for (Map.Entry<TableName, Routine> routineEntry : ais.getRoutines().entrySet()) {
                            TableName routineName = routineEntry.getKey();
                            if (routineName.getSchemaName().equals(schemaResolved))
                                routinesByName.put(routineName.getTableName(), routineEntry.getValue());
                        }
                        // Now, write them all out.
                        json.writeStartObject();
                        for (Map.Entry<String, Routine> routineEntry : routinesByName.entrySet()) {
                            json.writeFieldName(routineEntry.getKey());
                            writeProc(routineEntry.getValue(), json);
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
        json.writeStartObject(); {
            json.writeStringField("language", routine.getLanguage());
            json.writeStringField("calling_convention", routine.getCallingConvention().name());
            json.writeNumberField("max_dynamic_result_sets", routine.getDynamicResultSets());
            json.writeStringField("definition", routine.getDefinition());
            writeProcParams("parameters_in", routine.getParameters(), Parameter.Direction.IN, json);
            writeProcParams("parameters_out", routine.getParameters(), Parameter.Direction.OUT, json);
        } json.writeEndObject();
    }

    private void writeProcParams(String label, List<Parameter> parameters, Parameter.Direction direction,
                                 JsonGenerator json) throws IOException
    {
        json.writeArrayFieldStart(label);
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
                json.writeStartObject(); {
                    json.writeStringField("name", param.getName());
                    json.writeNumberField("position", i);
                    TInstance tInstance = param.tInstance();
                    TClass tClass = param.tInstance().typeClass();
                    json.writeStringField("type", tClass.name().unqualifiedName());
                    json.writeObjectFieldStart("type_options"); {
                        for (Attribute attr : tClass.attributes())
                            json.writeObjectField(attr.name().toLowerCase(), tInstance.attributeToObject(attr));
                    } json.writeEndObject();
                    json.writeBooleanField("is_inout", paramDir == Parameter.Direction.INOUT);
                    json.writeBooleanField("is_result", param.getDirection() == Parameter.Direction.RETURN);
                } json.writeEndObject();
            }
        }
        json.writeEndArray();
    }
}
