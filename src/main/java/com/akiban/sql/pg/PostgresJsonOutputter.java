
package com.akiban.sql.pg;

import static com.akiban.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.server.Quote;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.aksql.aktypes.AkResultSet;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;
import com.akiban.util.AkibanAppender;

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
    public void output(Row row, boolean usePVals) throws IOException {
        messenger.beginMessage(PostgresMessages.DATA_ROW_TYPE.code());
        messenger.writeShort(1);
        encoder.reset();
        outputRow(row, resultColumns, usePVals);
        ByteArrayOutputStream bytes = encoder.getByteStream();
        messenger.writeInt(bytes.size());
        messenger.writeByteStream(bytes);
        messenger.sendMessage();
    }

    protected void outputRow(Row row, List<JsonResultColumn> resultColumns, boolean usePVals)
            throws IOException {
        encoder.appendString("{");
        AkibanAppender appender = encoder.getAppender();
        int ncols = resultColumns.size();
        for (int i = 0; i < ncols; i++) {
            JsonResultColumn resultColumn = resultColumns.get(i);
            encoder.appendString((i == 0) ? "\"" : ",\"");
            Quote.DOUBLE_QUOTE.append(appender, resultColumn.getName());
            encoder.appendString("\":");
            if (usePVals) {
                PValueSource value = row.pvalue(i);
                TInstance columnTInstance = resultColumn.getTInstance();
                if (columnTInstance.typeClass() instanceof AkResultSet) {
                    outputNestedResultSet((Cursor)value.getObject(),
                                          resultColumn.getNestedResultColumns(),
                                          usePVals);
                }
                else {
                    columnTInstance.formatAsJson(value, appender);
                }
            }
            else {
                AkType type = resultColumn.getAkType();
                ValueSource value = row.eval(i);
                if (type == AkType.RESULT_SET) {
                    outputNestedResultSet(value.getResultSet(),
                                          resultColumn.getNestedResultColumns(),
                                          usePVals);
                }
                else if (value.isNull()) {
                    encoder.appendString("null");
                }
                else {
                    value.appendAsString(appender, Quote.JSON_QUOTE);
                }
            }
        }
        encoder.appendString("}");
    }

    protected void outputNestedResultSet(Cursor cursor, 
                                         List<JsonResultColumn> resultColumns,
                                         boolean usePVals) 
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
                outputRow(row, resultColumns, usePVals);
            }
        }
        finally {
            cursor.destroy();
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
