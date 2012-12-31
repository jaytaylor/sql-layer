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

package com.akiban.sql.pg;

import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Types;
import com.akiban.server.Quote;
import com.akiban.server.error.ExternalRoutineInvocationException;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.server.ServerJavaRoutine;
import com.akiban.sql.server.ServerJavaValues;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJavaRoutineJsonOutputter extends PostgresOutputter<ServerJavaRoutine>
{
    private Queue<ResultSet> resultSets;

    public PostgresJavaRoutineJsonOutputter(PostgresQueryContext context,
                                            PostgresJavaRoutine statement,
                                            Queue<ResultSet> resultSets) {
        super(context, statement);
        this.resultSets = resultSets;
    }

    @Override
    public void beforeData() throws IOException {
        if (context.getServer().getOutputFormat() == PostgresServerSession.OutputFormat.JSON_WITH_META_DATA) {
            outputMetaData();
        }
    }

    @Override
    public void output(ServerJavaRoutine javaRoutine, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        outputResults(javaRoutine, encoder.getAppender());
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    protected void outputResults(ServerJavaRoutine javaRoutine, AkibanAppender appender) throws IOException {
        encoder.appendString("{");
        boolean first = true;
        Routine routine = javaRoutine.getInvocation().getRoutine();
        List<Parameter> params = routine.getParameters();
        for (int i = 0; i < params.size(); i++) {
            Parameter param = params.get(i);
            if (param.getDirection() == Parameter.Direction.IN) continue;
            String name = param.getName();
            if (name == null)
                name = String.format("arg%d", i+1);
            Object value = javaRoutine.getOutParameter(param, i);
            outputValue(name, value, appender, first);
            first = false;
        }
        if (routine.getReturnValue() != null) {
            Object value = javaRoutine.getOutParameter(routine.getReturnValue(), 
                                                       ServerJavaValues.RETURN_VALUE_INDEX);
            outputValue("return", value, appender, first);
            first = false;
        }
        int i = 0;
        while (!resultSets.isEmpty()) {
            outputValue(String.format("result%d", ++i), resultSets.remove(),
                        appender, first);
            first = false;
        }
        encoder.appendString("}");
    }

    protected void outputValue(String name, Object value,
                               AkibanAppender appender, boolean first) 
            throws IOException {
        encoder.appendString(first ? "\"" : ",\"");
        Quote.DOUBLE_QUOTE.append(appender, name);
        encoder.appendString("\":");
            
        if (value == null) {
            encoder.appendString("null");
        }
        else if (value instanceof ResultSet) {
            try {
                outputResultSet((ResultSet)value, appender);
            }
            catch (SQLException ex) {
                throw new ExternalRoutineInvocationException(((PostgresJavaRoutine)statement).getInvocation().getRoutineName(), ex);
            }
        }
        else if ((value instanceof Boolean) ||
                 ((value instanceof Number) && !(value instanceof java.math.BigDecimal))) {
            encoder.appendString(value.toString());
        }
        else {
            encoder.appendString("\"");
            Quote.DOUBLE_QUOTE.append(appender, value.toString());
            encoder.appendString("\"");
        }
    }

    protected void outputResultSet(ResultSet resultSet, AkibanAppender appender) throws IOException, SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int ncols = metaData.getColumnCount();
        encoder.appendString("[");
        boolean first = true;
        while (resultSet.next()) {
            encoder.appendString(first ? "{" : ",{");
            for (int i = 0; i < ncols; i++) {
                String name = metaData.getColumnName(i+1);
                Object value = resultSet.getObject(i+1);
                outputValue(name, value, appender, (i == 0));
            }
            encoder.appendString("}");
            first = false;
        }
        resultSet.close();
        encoder.appendString("]");
    }
    
    public void outputMetaData() throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        try {
            outputMetaData(encoder.getAppender());
        }
        catch (SQLException ex) {
            throw new ExternalRoutineInvocationException(((PostgresJavaRoutine)statement).getInvocation().getRoutineName(), ex);
        }
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    protected void outputMetaData(AkibanAppender appender) throws IOException, SQLException {
        encoder.appendString("[");
        boolean first = true;
        Routine routine = ((PostgresJavaRoutine)statement).getInvocation().getRoutine();
        List<Parameter> params = routine.getParameters();
        for (int i = 0; i < params.size(); i++) {
            Parameter param = params.get(i);
            if (param.getDirection() == Parameter.Direction.IN) continue;
            String name = param.getName();
            if (name == null)
                name = String.format("arg%d", i+1);
            outputParameterMetaData(name, param, appender, first);
            first = false;
        }        
        if (routine.getReturnValue() != null) {
            outputParameterMetaData("return", routine.getReturnValue(), appender, first);
            first = false;
        }
        int i = 0;
        for (ResultSet resultSet : resultSets) {
            i++;
            outputResultSetMetaData(String.format("result%d", i), 
                                    resultSet.getMetaData(), appender, (i == 1));
        }
        encoder.appendString("]");
    }

    protected void outputParameterMetaData(String name, Parameter param,
                                           AkibanAppender appender, boolean first) 
            throws IOException {
        PostgresType pgType = PostgresType.fromAIS(param);

        if (!first)
            encoder.appendString(",");
        encoder.appendString("{\"name\":\"");
        Quote.DOUBLE_QUOTE.append(appender, name);
        encoder.appendString("\"");
        encoder.appendString(",\"oid\":");
        encoder.getWriter().print(pgType.getOid());
        encoder.appendString(",\"type\":");
        Quote.DOUBLE_QUOTE.append(appender, param.getTypeDescription());
        encoder.appendString("\"");
        if ((param.getType() == Types.DECIMAL) ||
            (param.getType() == Types.U_DECIMAL)) {
            encoder.appendString(",\"precision\":");
            encoder.getWriter().print(param.getTypeParameter1());
            encoder.appendString(",\"scale\":");
            encoder.getWriter().print(param.getTypeParameter1());
        }
        else if (!param.getType().fixedSize()) {
            encoder.appendString(",\"length\":");
            encoder.getWriter().print(param.getTypeParameter1());
        }
        encoder.appendString("}");
    }

    protected void outputResultSetMetaData(String name, ResultSetMetaData metaData,
                                           AkibanAppender appender, boolean first) 
            throws IOException, SQLException {
        if (!first)
            encoder.appendString(",");
        encoder.appendString("{\"name\":\"");
        Quote.DOUBLE_QUOTE.append(appender, name);
        encoder.appendString("\",columns:[");
        int ncols = metaData.getColumnCount();
        for (int i = 0; i < ncols; i++) {
            TypeId typeId = TypeId.getBuiltInTypeId(metaData.getColumnType(i+1));
            if (typeId == null) {
                try {
                    typeId = TypeId.getUserDefinedTypeId(metaData.getColumnTypeName(i+1),
                                                         false);
                }
                catch (StandardException ex) {
                    throw new SQLParserInternalException(ex);
                }
            }
            DataTypeDescriptor sqlType;
            if (typeId.isDecimalTypeId()) {
                sqlType = new DataTypeDescriptor(typeId,
                                                 metaData.getPrecision(i+1),
                                                 metaData.getScale(i+1),
                                                 metaData.isNullable(i+1) != ResultSetMetaData.columnNoNulls,
                                                 metaData.getColumnDisplaySize(i+1));
            }
            else {
                sqlType = new DataTypeDescriptor(typeId,
                                                 metaData.isNullable(i+1) != ResultSetMetaData.columnNoNulls,
                                                 metaData.getColumnDisplaySize(i+1));
            }
            PostgresType pgType = PostgresType.fromDerby(sqlType, null, null);

            if (i > 0)
                encoder.appendString(",");
            encoder.appendString("{\"name\":\"");
            Quote.DOUBLE_QUOTE.append(appender, metaData.getColumnName(i+1));
            encoder.appendString("\"");
            encoder.appendString(",\"oid\":");
            encoder.getWriter().print(pgType.getOid());
            encoder.appendString(",\"type\":");
            Quote.DOUBLE_QUOTE.append(appender, sqlType.toString());
            encoder.appendString("\"");
            if (typeId.isDecimalTypeId()) {
                encoder.appendString(",\"precision\":");
                encoder.getWriter().print(sqlType.getPrecision());
                encoder.appendString(",\"scale\":");
                encoder.getWriter().print(sqlType.getScale());
            }
            else if (typeId.variableLength()) {
                encoder.appendString(",\"length\":");
                encoder.getWriter().print(sqlType.getMaximumWidth());
            }
            encoder.appendString("}");
        }
        encoder.appendString("]}");
    }

}
