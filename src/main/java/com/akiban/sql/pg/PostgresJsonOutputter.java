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

import static com.akiban.sql.pg.PostgresJsonCompiler.JsonResultColumn;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.server.Quote;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

import java.util.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class PostgresJsonOutputter extends PostgresOutputter<Row>
{
    private List<JsonResultColumn> resultColumns;
    private PostgresType valueType;
    
    public PostgresJsonOutputter(PostgresQueryContext context, 
                                 PostgresBaseStatement statement,
                                 List<JsonResultColumn> resultColumns,
                                 PostgresType valueType) {
        super(context, statement);
        this.resultColumns = resultColumns;
        this.valueType = valueType;
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
                if (false) {
                    // TODO: No getResultSet() yet.
                }
                else if (value.isNull()) {
                    encoder.appendString("null");
                }
                else {
                    // TODO: No Quote support for new types.
                    encoder.appendString("\"");
                    encoder.appendPValue(value, valueType, false);
                    encoder.appendString("\"");
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

}
