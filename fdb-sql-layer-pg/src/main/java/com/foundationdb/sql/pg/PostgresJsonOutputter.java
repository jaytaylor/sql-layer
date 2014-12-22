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

import static com.foundationdb.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.Quote;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkResultSet;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import com.foundationdb.util.AkibanAppender;

import java.util.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJsonOutputter extends PostgresOutputter<Row>
{
    private List<JsonResultColumn> resultColumns;
    private PostgresType valueType;
    
    public PostgresJsonOutputter(PostgresQueryContext context, 
                                 PostgresDMLStatement statement,
                                 List<JsonResultColumn> resultColumns,
                                 PostgresType valueType) {
        super(context, statement);
        this.resultColumns = resultColumns;
        this.valueType = valueType;
    }

    @Override
    public void beforeData() throws IOException {
        if (context.getServer().getOutputFormat() == PostgresServerSession.OutputFormat.JSON_WITH_META_DATA)
            outputMetaData();
    }
    
    @Override
    public void output(Row row) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        outputRow(row, resultColumns);
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    protected void outputRow(Row row, List<JsonResultColumn> resultColumns)
            throws IOException {
        encoder.appendString("{");
        AkibanAppender appender = encoder.getAppender();
        int ncols = resultColumns.size();
        for (int i = 0; i < ncols; i++) {
            JsonResultColumn resultColumn = resultColumns.get(i);
            encoder.appendString((i == 0) ? "\"" : ",\"");
            Quote.DOUBLE_QUOTE.append(appender, resultColumn.getName());
            encoder.appendString("\":");
            ValueSource value = row.value(i);
            TInstance columnTInstance = resultColumn.getType();
            if (columnTInstance.typeClass() instanceof AkResultSet) {
                outputNestedResultSet((Cursor)value.getObject(),
                                      resultColumn.getNestedResultColumns());
            }
            else {
                FormatOptions options = context.getServer().getFormatOptions();
                columnTInstance.formatAsJson(value, appender, options);
            }
        }
        encoder.appendString("}");
    }

    protected void outputNestedResultSet(Cursor cursor, 
                                         List<JsonResultColumn> resultColumns) 
            throws IOException {
        encoder.appendString("[");
        try {
            Row row;
            boolean first = true;
            while ((row = cursor.next()) != null) {
                if (first)
                    first = false;
                else
                    encoder.appendString(",");
                outputRow(row, resultColumns);
            }
        }
        finally {
            if (!cursor.isClosed()) {
                cursor.close();
            }
        }
        encoder.appendString("]");
    }

    public void outputMetaData() throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        outputMetaData(resultColumns);
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    public void outputMetaData(List<JsonResultColumn> resultColumns) throws IOException {
        AkibanAppender appender = encoder.getAppender();
        encoder.appendString("[");
        boolean first = true;
        for (JsonResultColumn resultColumn : resultColumns) {
            if (first)
                first = false;
            else
                encoder.appendString(",");
            encoder.appendString("{\"name\":\"");
            Quote.DOUBLE_QUOTE.append(appender, resultColumn.getName());
            encoder.appendString("\"");
            if (resultColumn.getNestedResultColumns() != null) {
                encoder.appendString(",\"columns\":");
                outputMetaData(resultColumn.getNestedResultColumns());
            }
            else {
                if (resultColumn.getPostgresType() != null) {
                    encoder.appendString(",\"oid\":");
                    encoder.getWriter().print(resultColumn.getPostgresType().getOid());
                }
                if (resultColumn.getSqlType() != null) {
                    DataTypeDescriptor type = resultColumn.getSqlType();
                    encoder.appendString(",\"type\":\"");
                    Quote.DOUBLE_QUOTE.append(appender, type.toString());
                    encoder.appendString("\"");
                    TypeId typeId = type.getTypeId();
                    if (typeId.isDecimalTypeId()) {
                        encoder.appendString(",\"precision\":");
                        encoder.getWriter().print(type.getPrecision());
                        encoder.appendString(",\"scale\":");
                        encoder.getWriter().print(type.getScale());
                    }
                    else if (typeId.variableLength()) {
                        encoder.appendString(",\"length\":");
                        encoder.getWriter().print(type.getMaximumWidth());
                    }
                }
            }
            encoder.appendString("}");
        }
        encoder.appendString("]");
    }
}
