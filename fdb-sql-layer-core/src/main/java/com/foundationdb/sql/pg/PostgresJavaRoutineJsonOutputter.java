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

package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.server.Quote;
import com.foundationdb.server.error.ExternalRoutineInvocationException;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.server.ServerJavaRoutine;
import com.foundationdb.sql.server.ServerJavaValues;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

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
    public void output(ServerJavaRoutine javaRoutine) throws IOException {
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
            PostgresType pgType = PostgresType.fromAIS(param);
            outputValue(name, value, pgType, appender, first);
            first = false;
        }
        if (routine.getReturnValue() != null) {
            Object value = javaRoutine.getOutParameter(routine.getReturnValue(), 
                                                       ServerJavaValues.RETURN_VALUE_INDEX);
            PostgresType pgType = PostgresType.fromAIS(routine.getReturnValue());
            outputValue("return", value, pgType, appender, first);
            first = false;
        }
        int nresults = 0;
        while (!resultSets.isEmpty()) {
            String name = (nresults++ > 0) ? String.format("result_set_%d", nresults) : "result_set";
            outputValue(name, resultSets.remove(), null, appender, first);
            first = false;
        }
        encoder.appendString("}");
    }

    protected void outputValue(String name, Object value, PostgresType pgType,
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
        else {
            ValueSource source = encoder.valuefromObject(value, pgType);
            FormatOptions options = context.getServer().getFormatOptions();
            pgType.getType().formatAsJson(source, appender, options);
        }
    }

    protected void outputResultSet(ResultSet resultSet, AkibanAppender appender) throws IOException, SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int ncols = metaData.getColumnCount();
        PostgresType[] pgTypes = new PostgresType[ncols];
        for (int i = 0; i < ncols; i++) {
            DataTypeDescriptor sqlType = resultColumnSQLType(metaData, i+1);
            pgTypes[i] = PostgresType.fromDerby(sqlType, null);
        }
        encoder.appendString("[");
        boolean first = true;
        while (resultSet.next()) {
            encoder.appendString(first ? "{" : ",{");
            for (int i = 0; i < ncols; i++) {
                String name = metaData.getColumnName(i+1);
                Object value = resultSet.getObject(i+1);
                outputValue(name, value, pgTypes[i], appender, (i == 0));
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
        encoder.appendString(",\"type\":\"");
        Quote.DOUBLE_QUOTE.append(appender, param.getTypeDescription());
        encoder.appendString("\"");
        if (pgType.getModifier() > 0) {
            int mod = pgType.getModifier() - 4;
            if (pgType.getOid() == PostgresType.TypeOid.NUMERIC_TYPE_OID.getOid()) {
                encoder.appendString(",\"precision\":");
                encoder.getWriter().print(mod >> 16);
                encoder.appendString(",\"scale\":");
                encoder.getWriter().print(mod & 0xFFFF);
            }
            else {
                encoder.appendString(",\"length\":");
                encoder.getWriter().print(mod);
            }
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
            DataTypeDescriptor sqlType = resultColumnSQLType(metaData, i+1);
            PostgresType pgType = PostgresType.fromDerby(sqlType, null);

            if (i > 0)
                encoder.appendString(",");
            encoder.appendString("{\"name\":\"");
            Quote.DOUBLE_QUOTE.append(appender, metaData.getColumnName(i+1));
            encoder.appendString("\"");
            encoder.appendString(",\"oid\":");
            encoder.getWriter().print(pgType.getOid());
            encoder.appendString(",\"type\":\"");
            Quote.DOUBLE_QUOTE.append(appender, sqlType.toString());
            encoder.appendString("\"");
            if (sqlType.getTypeId().isDecimalTypeId()) {
                encoder.appendString(",\"precision\":");
                encoder.getWriter().print(sqlType.getPrecision());
                encoder.appendString(",\"scale\":");
                encoder.getWriter().print(sqlType.getScale());
            }
            else if (sqlType.getTypeId().variableLength()) {
                encoder.appendString(",\"length\":");
                encoder.getWriter().print(sqlType.getMaximumWidth());
            }
            encoder.appendString("}");
        }
        encoder.appendString("]}");
    }

    protected DataTypeDescriptor resultColumnSQLType(ResultSetMetaData metaData, int i)
            throws SQLException {
        TypeId typeId = TypeId.getBuiltInTypeId(metaData.getColumnType(i));
        if (typeId == null) {
            try {
                typeId = TypeId.getUserDefinedTypeId(metaData.getColumnTypeName(i),
                                                     false);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
        }
        if (typeId.isDecimalTypeId()) {
            return new DataTypeDescriptor(typeId,
                                          metaData.getPrecision(i),
                                          metaData.getScale(i),
                                          metaData.isNullable(i) != ResultSetMetaData.columnNoNulls,
                                          metaData.getColumnDisplaySize(i));
            }
        else {
            return new DataTypeDescriptor(typeId,
                                          metaData.isNullable(i) != ResultSetMetaData.columnNoNulls,
                                          metaData.getColumnDisplaySize(i));
        }
    }

}
