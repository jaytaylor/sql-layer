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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.IndexColumn;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonRowWriter
{
    private static final Logger logger = LoggerFactory.getLogger(JsonRowWriter.class);

    private final RowTracker tracker;

    public JsonRowWriter(RowTracker tracker) {
        this.tracker = tracker;
    }

    public boolean writeRows(Cursor cursor, AkibanAppender appender, String prefix, WriteRow rowWriter) {
        cursor.open();
        tracker.reset();
        final int minDepth = tracker.getMinDepth();
        final int maxDepth = tracker.getMaxDepth();
        int depth = minDepth - 1;
        Row row;
        while ((row = cursor.next()) != null) {
            logger.trace("Row {}", row);
            tracker.beginRow(row);
            int rowDepth = tracker.getRowDepth();
            boolean begun = false;
            if (depth >= rowDepth) {
                if (tracker.isSameRowType())
                    begun = true;
                do {
                    appender.append((depth > rowDepth || !begun) ? "}]" : "}");
                    depth--;
                } while (depth >= rowDepth);
            }
            if (rowDepth > maxDepth)
                continue;
            assert (rowDepth == depth+1);
            depth = rowDepth;
            tracker.pushRowType();
            if (begun) {
                appender.append(',');
            }
            else if (depth > minDepth) {
                appender.append(",\"");
                appender.append(tracker.getRowName());
                appender.append("\":[");
            }
            else {
                appender.append(prefix);
            }
            appender.append('{');
            rowWriter.write(row, appender);
        }
        cursor.close();
        if (depth < minDepth)
            return false;       // Cursor was empty = not found.
        do {
            appender.append((depth > minDepth) ? "}]" : "}");
            depth--;
        } while (depth >= minDepth);
        return true;
    }

    public static void writeValue(String name, PValueSource pvalue, AkibanAppender appender, boolean first) {
        if(!first) {
            appender.append(',');
        }
        appender.append('"');
        appender.append(name);
        appender.append("\":");
        pvalue.tInstance().formatAsJson(pvalue, appender);
    }

    /**
     * Write the name:value pairs of the data from a row into Json format.
     * Current implementations take names from the table columns or the
     * table's primary key columns. 
     * @author tjoneslo
     */
    public interface WriteRow {
        public void write(Row row, AkibanAppender appender);

    }
    
    public static class WriteTableRow implements WriteRow {
        @Override
        public void write(Row row, AkibanAppender appender) {
            List<Column> columns = row.rowType().userTable().getColumns();
            for (int i = 0; i < columns.size(); i++) {
                writeValue(columns.get(i).getName(), row.pvalue(i), appender, i == 0);
             }
        }
    }

    public static class WriteCapturePKRow implements WriteRow {
        private Map<Column, PValueSource> pkValues = new HashMap<>();

        @Override
        public void write(Row row, AkibanAppender appender) {
            // tables with hidden PK (noPK tables) return no values
            if (row.rowType().userTable().getPrimaryKey() == null) return;
            
            List<IndexColumn> columns = row.rowType().userTable().getPrimaryKey().getIndex().getKeyColumns();
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i).getColumn();
                writeValue(column.getName(), row.pvalue(column.getPosition()), appender, i == 0);
                pkValues.put(column, row.pvalue(column.getPosition()));
            }
        }

        public Map<Column, PValueSource> getPKValues() {
            return pkValues;
        }
    }
}
