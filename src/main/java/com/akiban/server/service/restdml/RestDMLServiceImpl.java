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

package com.akiban.server.service.restdml;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.jonquil.Jonquil;
import com.akiban.jonquil.JonquilException;
import com.akiban.jonquil.JonquilWriter;
import com.akiban.jonquil.JoinFields;
import com.akiban.jonquil.JoinStrategy;
import com.akiban.jonquil.actions.Action;
import com.akiban.server.Quote;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.InvalidArgumentTypeException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.explain.format.JsonFormatter;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.externaldata.ExternalDataService;
import com.akiban.server.service.externaldata.JsonRowWriter;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.text.FullTextIndexService;
import com.akiban.server.service.text.FullTextQueryBuilder;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.embedded.EmbeddedJDBCService;
import com.akiban.sql.embedded.JDBCCallableStatement;
import com.akiban.sql.embedded.JDBCConnection;
import com.akiban.sql.embedded.JDBCParameterMetaData;
import com.akiban.sql.embedded.JDBCResultSet;
import com.akiban.util.AkibanAppender;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.akiban.server.service.transaction.TransactionService.CloseableTransaction;
import static com.akiban.util.JsonUtils.createJsonGenerator;
import static com.akiban.util.JsonUtils.jsonParser;

public class RestDMLServiceImpl implements Service, RestDMLService {
    private final SessionService sessionService;
    private final DXLService dxlService;
    private final TransactionService transactionService;
    private final ExternalDataService extDataService;
    private final EmbeddedJDBCService jdbcService;
    private final InsertProcessor insertProcessor;
    private final DeleteProcessor deleteProcessor;
    private final UpdateProcessor updateProcessor;
    private final UpsertProcessor upsertProcessor;
    private final FullTextIndexService fullTextService;
    private static final InOutTap ENTITY_GET = Tap.createTimer("rest: entity GET");
    private static final InOutTap ENTITY_POST = Tap.createTimer("rest: entity POST");
    private static final InOutTap ENTITY_PUT = Tap.createTimer("rest: entity PUT");
    private static final InOutTap ENTITY_DELETE = Tap.createTimer("rest: entity DELETE");
    private static final InOutTap ENTITY_PATCH = Tap.createTimer("rest: entity PATCH");
    private static final InOutTap ENTITY_SQL = Tap.createTimer("rest: entity sql");
    private static final InOutTap ENTITY_EXPLAIN = Tap.createTimer("rest: entity explain");
    private static final InOutTap ENTITY_PARAM = Tap.createTimer("rest: entity sql parameter");
    
    private static final InOutTap ENTITY_TEXT = Tap.createTimer("rest: entity text");
    private static final InOutTap ENTITY_CALL = Tap.createTimer("rest: entity call");


    @Inject
    public RestDMLServiceImpl(SessionService sessionService,
                              DXLService dxlService,
                              TransactionService transactionService,
                              ExternalDataService extDataService,
                              EmbeddedJDBCService jdbcService,
                              FullTextIndexService fullTextService,
                              ConfigurationService configService,
                              TreeService treeService,
                              Store store,
                              T3RegistryService registryService) {
        this.sessionService = sessionService;
        this.dxlService = dxlService;
        this.transactionService = transactionService;
        this.extDataService = extDataService;
        this.jdbcService = jdbcService;
        this.fullTextService = fullTextService;
        this.insertProcessor = new InsertProcessor (store, registryService);
        this.deleteProcessor = new DeleteProcessor (store, registryService);
        this.updateProcessor = new UpdateProcessor (store, registryService, deleteProcessor, insertProcessor);
        this.upsertProcessor = new UpsertProcessor (store, registryService, insertProcessor, extDataService);
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
        //None
    }

    /* RestDMLService */

    @Override
    public void getAllEntities(PrintWriter writer, TableName tableName, Integer depth) {
        int realDepth = (depth != null) ? Math.max(depth, 0) : -1;
        ENTITY_GET.in();
        try (Session session = sessionService.createSession()) {
            extDataService.dumpAllAsJson(session,
                    writer,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    realDepth,
                    true);
        } finally {
            ENTITY_GET.out();
        }
    }

    @Override
    public void getEntities(PrintWriter writer, TableName tableName, Integer depth, String identifiers) {
        int realDepth = (depth != null) ? Math.max(depth, 0) : -1;
        ENTITY_GET.in();
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            UserTable uTable = dxlService.ddlFunctions().getUserTable(session, tableName);
            Index pkIndex = uTable.getPrimaryKeyIncludingInternal().getIndex();
            List<List<String>> pks = PrimaryKeyParser.parsePrimaryKeys(identifiers, pkIndex);
            extDataService.dumpBranchAsJson(session,
                    writer,
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    pks,
                    realDepth,
                    false);
            txn.commit();
        } finally {
            ENTITY_GET.out();
        }
    }

    @Override
    public void insert(PrintWriter writer, TableName tableName, JsonNode node)  {
        ENTITY_POST.in();
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            insertNoTxn(session, writer, tableName, node);
            txn.commit();
        } finally {
            ENTITY_POST.out();
        }
    }

    @Override
    public void delete(TableName tableName, String identifier) {
        ENTITY_DELETE.in();
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
            deleteProcessor.processDelete(session, ais, tableName, identifier);
            txn.commit();
        } finally {
            ENTITY_DELETE.out();
        }
    }

    @Override
    public void update(PrintWriter writer, TableName tableName, String pks, JsonNode node) {
        ENTITY_PUT.in();
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            updateNoTxn(session, writer, tableName, pks, node);
            txn.commit();
        } finally {
            ENTITY_PUT.out();
        }
    }

    @Override
    public void upsert(PrintWriter writer, TableName tableName, JsonNode node) {
        ENTITY_PATCH.in();
        try (Session session = sessionService.createSession();
                CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
            String pk = upsertProcessor.processUpsert(session, ais, tableName, node);
            writer.write(pk);
            txn.commit();
        } finally {
            ENTITY_PATCH.out();
        }
    }

    @Override
    public void insertNoTxn(Session session, PrintWriter writer, TableName tableName, JsonNode node) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        String pk = insertProcessor.processInsert(session, ais, tableName, node);
        if(writer != null) {
            writer.write(pk);
        }
    }

    @Override
    public void updateNoTxn(Session session, PrintWriter writer, TableName tableName, String pks, JsonNode node) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        String pk = updateProcessor.processUpdate(session, ais, tableName, pks, node);
        writer.write(pk);
    }

    @Override
    public void runSQL(PrintWriter writer, HttpServletRequest request, String sql, String schema) throws SQLException {
        runSQLFlat(writer, request, Collections.singletonList(sql), schema, OutputType.ARRAY, CommitMode.AUTO);
    }

    @Override
    public void runSQL(PrintWriter writer, HttpServletRequest request, List<String> sql) throws SQLException {
        runSQLFlat(writer, request, sql, null, OutputType.OBJECT, CommitMode.MANUAL);
    }

    @Override
    public void runSQLParameter(PrintWriter writer,HttpServletRequest request, String sql, List<String> parameters) throws SQLException {
        runSQLParameterized (writer, request, sql, parameters, OutputType.ARRAY, CommitMode.AUTO); 
    }

    @Override
    public void explainSQL(PrintWriter writer, HttpServletRequest request, String sql) throws IOException, SQLException {
        ENTITY_EXPLAIN.in();
        try (JDBCConnection conn = jdbcConnection(request)) {
            new JsonFormatter().format(conn.explain(sql), writer);
        } finally {
            ENTITY_EXPLAIN.out();
        }
    }

    @Override
    public void callProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                                 TableName procName, Map<String,List<String>> queryParams, String content) throws SQLException {
        ENTITY_CALL.in();
        try (JDBCConnection conn = jdbcConnection(request, procName.getSchemaName());
             JDBCCallableStatement call = conn.prepareCall(procName)) {
            Routine routine = call.getRoutine();
            switch (routine.getCallingConvention()) {
            case SCRIPT_FUNCTION_JSON:
            case SCRIPT_BINDINGS_JSON:
                callJsonProcedure(writer, request, jsonpArgName, call, queryParams, content);
                break;
            default:
                callDefaultProcedure(writer, request, jsonpArgName, call, queryParams, content);
                break;
            }
        } finally {
            ENTITY_CALL.out();
        }
    }

    protected void callJsonProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                                     JDBCCallableStatement call, Map<String,List<String>> queryParams, String jsonParams) throws SQLException {
        String json = null;
        if (jsonParams != null) {
            json = jsonParams;
        }
        else if (queryParams != null) {
            // Stringify query params as JSON.
            // TODO: Is this even a good idea? Or just keep them separate?
            try {
                StringWriter str = new StringWriter();
                JsonGenerator jg = createJsonGenerator(str);
                jg.writeStartObject();
                for (Map.Entry<String,List<String>> entry : queryParams.entrySet()) {
                    if (jsonpArgName.equals(entry.getKey()))
                        continue;
                    jg.writeFieldName(entry.getKey());
                    if (entry.getValue().size() != 1)
                        jg.writeStartArray();
                    for (String value : entry.getValue())
                        jg.writeString(value);
                    if (entry.getValue().size() != 1)
                        jg.writeEndArray();
                }
                jg.writeEndObject();
                jg.close();
                json = str.toString();
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error writing to string", ex);
            }
        }
        call.setString(1, json);
        call.execute();
        writer.append(call.getString(2));
    }

    protected void callDefaultProcedure(PrintWriter writer, HttpServletRequest request, String jsonpArgName,
                                        JDBCCallableStatement call, Map<String,List<String>> queryParams, String jsonParams) throws SQLException {
        if (queryParams != null) {
            for (Map.Entry<String,List<String>> entry : queryParams.entrySet()) {
                if (jsonpArgName.equals(entry.getKey()))
                    continue;
                if (entry.getValue().size() != 1)
                    throw new WrongExpressionArityException(1, entry.getValue().size());
                call.setString(entry.getKey(), entry.getValue().get(0));
            }
        }
        if (jsonParams != null) {
            JsonNode parsed;
            try {
                parsed = jsonParser(jsonParams).readValueAsTree();
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error reading from string", ex);
            }
            if (parsed.isObject()) {
                Iterator<String> iter = parsed.fieldNames();
                while (iter.hasNext()) {
                    String field = iter.next();
                    JsonNode value = parsed.get(field);
                    if (value.isBigDecimal()) {
                        call.setBigDecimal(field, value.decimalValue());
                    }
                    else if (value.isBoolean()) {
                        call.setBoolean(field, value.asBoolean());
                    }
                    else if (value.isDouble()) {
                        call.setDouble(field, value.asDouble());
                    }
                    else if (value.isInt()) {
                        call.setInt(field, value.asInt());
                    }
                    else if (value.isLong()) {
                        call.setLong(field, value.asLong());
                    }
                    else {
                        call.setString(field, value.textValue());
                    }
                }
            }
            else {
                throw new InvalidArgumentTypeException("JSON must be object or array");
            }
        }
        boolean results = call.execute();
        AkibanAppender appender = AkibanAppender.of(writer);
        appender.append('{');
        boolean first = true;
        JDBCParameterMetaData md = (JDBCParameterMetaData)call.getParameterMetaData();
        for (int i = 1; i <= md.getParameterCount(); i++) {
            String name;
            switch (md.getParameterMode(i)) {
            case ParameterMetaData.parameterModeOut:
            case ParameterMetaData.parameterModeInOut:
                name = md.getParameterName(i);
                if (name == null)
                    name = String.format("arg%d", i);
                if (first)
                    first = false;
                else
                    appender.append(',');
                appender.append('"');
                Quote.DOUBLE_QUOTE.append(appender, name);
                appender.append("\":");
                call.formatAsJson(i, appender);
                break;
            }
        }
        int nresults = 0;
        while(results) {
            beginResultSetArray(appender, first, nresults++);
            first = false;
            collectResults((JDBCResultSet) call.getResultSet(), appender);
            endResultSetArray(appender);
            results = call.getMoreResults();
        }
        appender.append('}');
    }

    @Override
    public String jonquilToSQL(TableName tableName, String jonquil) throws IOException {
        final AkibanInformationSchema ais;
        try (Session session = sessionService.createSession();
             CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
            ais = dxlService.ddlFunctions().getAIS(session);
            txn.commit();
        }
        JsonParser json = oldJsonFactory.createJsonParser(jonquil);
        final String schema = tableName.getSchemaName();
        // the JoinStrategy will assume all tables are in the same schema
        JoinStrategy joinStrategy = new JoinStrategy() {
            @Override
            public Collection<? extends JoinFields> getJoins(String parent, String child) {
                UserTable parentTable = ais.getUserTable(schema, parent);
                if (parentTable == null)
                    throw new NoSuchElementException("parent table: " + parent);
                Join groupingJoin = findGroupingJoin(parentTable, child);
                return Lists.transform(groupingJoin.getJoinColumns(), new Function<JoinColumn, JoinFields>() {
                    @Override
                    public JoinFields apply(JoinColumn input) {
                        return new JoinFields(input.getParent().getName(), input.getChild().getName());
                    }
                });
            }

            private Join findGroupingJoin(UserTable parentTable, String childTable) {
                for (Join join : parentTable.getChildJoins()) {
                    if (join.getChild().getName().getTableName().equals(childTable))
                        return join;
                }
                throw new NoSuchElementException("no child named " + childTable + " for table " + parentTable);
            }
        };
        Map<String, Action> additionalActionsMap = new HashMap<>();
        additionalActionsMap.put("fields", new Action() {
            @Override
            public void apply(JsonParser input, JonquilWriter output, String tableName) throws IOException {
                JsonToken token = input.nextToken();
                if (token == JsonToken.VALUE_STRING) {
                    String value = input.getText();
                    if (value.equalsIgnoreCase("all")) {
                        UserTable table = ais.getUserTable(schema, tableName);
                        for (Column column : table.getColumns())
                            output.addScalar(column.getName());
                    }
                    else {
                        throw new JonquilException("illegal string value for @fields (must be \"all\"): " + value);
                    }
                }
                else if (token == JsonToken.START_ARRAY) {
                    while (input.nextToken() != JsonToken.END_ARRAY) {
                        if (input.getCurrentToken() != JsonToken.VALUE_STRING) {
                            throw new JonquilException("illegal value for @attributes list: "
                                    + input.getText() + " (" + token + ')');
                        }
                        output.addScalar(input.getText());
                    }
                }
                else {
                    throw new JonquilException("illegal value for @fields: " + input.getText() + " (" + token + ')');
                }
            }
        });
        Function<String, Action> additionalActions = Functions.forMap(additionalActionsMap, null);
        return Jonquil.createSQL(tableName.getTableName(), json, joinStrategy, additionalActions);
    }

    public interface ProcessStatement {
        public Statement processStatement (int index) throws SQLException; 
    }
    
    private void runSQLParameterized(PrintWriter writer,
                                    HttpServletRequest request, String sql, List<String> params,
                                    OutputType outputType, CommitMode commitMode) throws SQLException {
        ENTITY_PARAM.in();
        try (Connection conn = jdbcConnection(request);
                final PreparedStatement s = conn.prepareStatement(sql)) {
            int index = 1;
            for (String param : params) {
                s.setString(index++, param);
            }
            processSQL (conn, writer, outputType, commitMode,
                    new ProcessStatement() {
                    @Override
                    public Statement processStatement(int index) throws SQLException {
                        if (index == 0) {
                            s.execute();
                            return s;
                        } else {
                            return null;
                        }
                    }
            });
        } finally {
            ENTITY_PARAM.out();
        }
    }

    private void runSQLFlat(PrintWriter writer,
            HttpServletRequest request, final List<String> sqlList, String schema,
            OutputType outputType, CommitMode commitMode) throws SQLException {
        ENTITY_SQL.in();
        try (JDBCConnection conn = jdbcConnection(request);
              final Statement s = conn.createStatement()) {
            if (schema != null)
                conn.setProperty("database", schema);
            processSQL (conn, writer, outputType, commitMode,
                    new ProcessStatement() {
                        private int offset = 0;
                        @Override
                        public Statement processStatement(int index) throws SQLException {
                            if (index + offset < sqlList.size()) {
                                String trimmed = sqlList.get(index + offset).trim();
                                while (trimmed.isEmpty() && sqlList.size() < index + offset) {
                                    ++offset;
                                    trimmed = sqlList.get(index + offset).trim();
                                }
                                if (!trimmed.isEmpty()) {
                                    s.execute(trimmed);
                                    return s;
                                }
                            }
                            return null;
                        }
            });
        } finally {
            ENTITY_SQL.out();
        }
    }

   private void processSQL (Connection conn, PrintWriter writer, 
            OutputType outputType, CommitMode commitMode, 
            ProcessStatement stmt) throws SQLException {
        boolean useSubArrays = (outputType == OutputType.OBJECT);
        AkibanAppender appender = AkibanAppender.of(writer);
        int nresults = 0;
        commitMode.begin(conn);
        outputType.begin(appender);
        
        Statement s = stmt.processStatement(nresults);
        while (s != null) {
            if(useSubArrays) {
                beginResultSetArray(appender, nresults == 0, nresults);
            }
            JDBCResultSet results = (JDBCResultSet) s.getResultSet();
            int updateCount = s.getUpdateCount();
            
            if (results != null && !results.isClosed()) {
                collectResults((JDBCResultSet)s.getResultSet(), appender);
                // Force close the result set here because if you execute "SELECT...;INSERT..." 
                // the call to s.getResultSet() returns the (now empty) SELECT result set
                // giving bad results
                results.close();
            } else {
                appender.append("\n{\"update_count\":");
                appender.append(updateCount);
                appender.append("}\n");
            }
            if(useSubArrays) {
                endResultSetArray(appender);
            }
            ++nresults;
            s = stmt.processStatement(nresults);
        }
        
        commitMode.end(conn);
        outputType.end(appender);

    }
    
    private static void beginResultSetArray(AkibanAppender appender, boolean first, int resultOffset) {
        String name = (resultOffset == 0) ? "result_set" : String.format("result_set_%d", resultOffset);
        if(!first) {
            appender.append(",");
        }
        appender.append('"');
        appender.append(name);
        appender.append("\":[");
    }

    private static void endResultSetArray(AkibanAppender appender) {
        appender.append(']');
    }

    private static void collectResults(JDBCResultSet resultSet, AkibanAppender appender) throws SQLException {
        SQLOutputCursor cursor = new SQLOutputCursor(resultSet);
        JsonRowWriter jsonRowWriter = new JsonRowWriter(cursor);
        if(jsonRowWriter.writeRows(cursor, appender, "\n", cursor)) {
            appender.append('\n');
        }
    }

    private JDBCConnection jdbcConnection(HttpServletRequest request) throws SQLException {
        return (JDBCConnection) jdbcService.newConnection(new Properties(), request.getUserPrincipal());
    }

    private JDBCConnection jdbcConnection(HttpServletRequest request, String schemaName) throws SQLException {
        // TODO: This is to make up for test situations where the
        // request is not authenticated.
        Properties properties = new Properties();
        if ((request.getUserPrincipal() == null) &&
            (schemaName != null)) {
            properties.put("user", schemaName);
        }
        return (JDBCConnection) jdbcService.newConnection(properties, request.getUserPrincipal());
    }

    private static enum OutputType {
        ARRAY('[', ']'),
        OBJECT('{', '}');

        public void begin(AkibanAppender appender) {
            appender.append(beginChar);
        }

        public void end(AkibanAppender appender) {
            appender.append(endChar);
        }

        private OutputType(char beginChar, char endChar) {
            this.beginChar = beginChar;
            this.endChar = endChar;
        }

        public final char beginChar;
        public final char endChar;
    }

    private static enum CommitMode {
        AUTO,
        MANUAL;

        public void begin(Connection conn) throws SQLException {
            conn.setAutoCommit(this == AUTO);
        }

        public void end(Connection conn) throws SQLException {
            if(this == MANUAL) {
                conn.commit();
            }
        }
    }

    @Override
    public void fullTextSearch(PrintWriter writer, IndexName indexName, Integer depth, String query, Integer limit) {
        int realDepth = (depth != null) ? Math.max(depth, 0) : -1;
        int realLimit = (limit != null) ? limit.intValue() : -1;
        ENTITY_TEXT.in();
        FullTextQueryBuilder builder = new FullTextQueryBuilder(indexName, 
                                                                fullTextService);
        try (Session session = sessionService.createSession()) {
            extDataService.dumpBranchAsJson(session,
                                            writer,
                                            indexName.getSchemaName(),
                                            indexName.getTableName(),
                                            builder.scanOperator(query, realLimit),
                                            fullTextService.searchRowType(session, indexName),
                                            realDepth,
                                            true);
        } finally {
            ENTITY_TEXT.out();
        }
    }
    
     private static final JsonFactory oldJsonFactory = new JsonFactory(new ObjectMapper()); // for Jonquil
}
