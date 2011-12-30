/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import static com.akiban.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.server.Quote;
import com.akiban.server.types.AkType;
import com.akiban.util.AkibanAppender;

import java.util.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJsonOutputter extends PostgresOutputter<Row>
{
    private List<JsonResultColumn> resultColumns;
    private PostgresType valueType;
    
    public PostgresJsonOutputter(PostgresMessenger messenger, 
                                 PostgresBaseStatement statement,
                                 List<JsonResultColumn> resultColumns,
                                 PostgresType valueType) {
        super(messenger, statement);
        this.resultColumns = resultColumns;
        this.valueType = valueType;
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
            AkType type = resultColumn.getAkType();
            if (type == AkType.RESULT_SET) {
                outputNestedResultSet(row.eval(i).getResultSet(),
                                      resultColumn.getNestedResultColumns());
            }
            else {
                Quote.JSON_QUOTE.quote(appender, type);
                encoder.appendValue(row.eval(i), valueType, false);
                Quote.JSON_QUOTE.quote(appender, type);
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
            while ((row = cursor.next()) != null) {
                outputRow(row, resultColumns);
            }
        }
        finally {
            cursor.close();
        }
        encoder.appendString("]");
    }

}
