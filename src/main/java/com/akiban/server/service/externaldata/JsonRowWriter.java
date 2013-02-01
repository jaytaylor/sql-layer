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
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.util.AkibanAppender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class JsonRowWriter
{
    private int minDepth, maxDepth;
    // This is not sufficient if orphans are possible (when
    // ancestor keys are repeated in descendants). In that case, we
    // have to save rows and check that they are ancestors of new
    // rows, discarding any that are not.
    private RowType[] openTypes;

    public JsonRowWriter(UserTable table, int addlDepth) {
        minDepth = maxDepth = table.getDepth();
        if (addlDepth < 0) {
            table.traverseTableAndDescendants(new NopVisitor() {
                    @Override
                    public void visitUserTable(UserTable userTable) {
                        maxDepth = Math.max(maxDepth, userTable.getDepth());
                    }
                });
        }
        else {
            maxDepth += addlDepth;
        }
        openTypes = new RowType[maxDepth+1];
    }

    private static final Logger logger = LoggerFactory.getLogger(JsonRowWriter.class);

    public boolean writeRows(Cursor cursor, AkibanAppender appender, String prefix, WriteRow rowWriter)
            throws IOException {
        cursor.open();
        int depth = minDepth-1;
        Row row;
        while ((row = cursor.next()) != null) {
            logger.trace("Row {}", row);
            RowType rowType;
            UserTable table; 
            
            if (row.rowType().hasUserTable()) {
                rowType = row.rowType();
                table = rowType.userTable();
            } else {
                throw new RuntimeException ("Invaid row type for JsonRowWriter#writeRows()");
            }
            
            int rowDepth = table.getDepth();
            boolean begun = false;
            if (depth >= rowDepth) {
                if (rowType == openTypes[rowDepth])
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
            openTypes[depth] = rowType;
            if (begun) {
                appender.append(',');
            }
            else if (depth > minDepth) {
                appender.append(",\"");
                appender.append(table.getName().toString());
                appender.append("\":[");
            }
            else {
                appender.append(prefix);
            }
            appender.append('{');
            rowWriter.write(row, appender);
/*
            List<Column> columns = table.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) appender.append(',');
                appender.append('"');
                appender.append(columns.get(i).getName());
                appender.append("\":");
                PValueSource pvalue = row.pvalue(i);
                pvalue.tInstance().formatAsJson(pvalue, appender);
            }
*/            
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
    
    /**
     * Write the name:value pairs of the data from a row into Json format.
     * Current implementations take names from the table columns or the
     * table's primary key columns. 
     * @author tjoneslo
     *
     */
    public static abstract class WriteRow {
        public abstract void write(Row row, AkibanAppender appender);
        protected void writeValue (String name, PValueSource pvalue, AkibanAppender appender) {
            appender.append('"');
            appender.append(name);
            appender.append("\":");
            pvalue.tInstance().formatAsJson(pvalue, appender);
        }
    }
    
    public static class WriteTableRow extends WriteRow {
        public WriteTableRow () {}
        
        public void write(Row row, AkibanAppender appender) {
            List<Column> columns = row.rowType().userTable().getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) appender.append(',');
                writeValue (columns.get(i).getName(), row.pvalue(i), appender);
             }
        }
    }
    
    public static class WritePKRow extends WriteRow {
        public WritePKRow () {}
        
        public void write(Row row, AkibanAppender appender) {
            List<Column> columns = row.rowType().userTable().getPrimaryKey().getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) appender.append(',');
                writeValue(columns.get(i).getName(), row.pvalue(i), appender);
            }
        }
    }
}
