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

package com.akiban.qp.loadableplan.std;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.loadableplan.DirectObjectCursor;
import com.akiban.qp.loadableplan.DirectObjectPlan;
import com.akiban.qp.loadableplan.LoadableDirectObjectPlan;
import com.akiban.qp.operator.BindingNotSetException;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.types.util.SqlLiteralValueFormatter;

import java.sql.Types;

import java.io.IOException;
import java.util.*;

/**
 * Output table contents in group order.
 */
public class DumpGroupLoadablePlan extends LoadableDirectObjectPlan
{
    @Override
    public String name() {
        return "dump_group";
    }

    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context) {
                return new DumpGroupDirectObjectCursor(context);
            }

            @Override
            public boolean useCopyData() {
                // Output as raw text, not rows.
                return true;
            }
        };
    }

    public static final int CHARS_PER_MESSAGE = 1000;
    public static final int MESSAGES_PER_FLUSH = 100;

    public static class DumpGroupDirectObjectCursor extends DirectObjectCursor {
        private final QueryContext context;
        private UserTable rootTable;
        private Cursor cursor;
        private Map<UserTable,Integer> tableSizes;
        private StringBuilder buffer;
        private GroupRowFormatter formatter;
        private int messagesSent;

        public DumpGroupDirectObjectCursor(QueryContext context) {
            this.context = context;
        }

        @Override
        public void open() {
            String currentSchema = context.getCurrentUser();
            String schemaName, tableName;
            if (context.getValue(0).isNull())
                schemaName = currentSchema;
            else
                schemaName = context.getValue(0).getString();
            tableName = context.getValue(1).getString();
            rootTable = context.getStore().schema().ais()
                .getUserTable(schemaName, tableName);
            if (rootTable == null)
                throw new NoSuchTableException(schemaName, tableName);
            cursor = context.getStore(rootTable)
                .newGroupCursor(rootTable.getGroup().getGroupTable());
            cursor.open();
            tableSizes = new HashMap<UserTable,Integer>();
            buffer = new StringBuilder();
            formatter = new SQLRowFormatter(buffer, currentSchema);
            messagesSent = 0;
        }

        @Override
        public List<String> next() {
            if (messagesSent >= MESSAGES_PER_FLUSH) {
                messagesSent = 0;
                return Collections.emptyList();
            }
            while (cursor.isActive()) {
                Row row = cursor.next();
                if (row == null) {
                    cursor.close();
                    break;
                }
                RowType rowType = row.rowType();
                UserTable rowTable = rowType.userTable();
                int size = tableSize(rowTable);
                if (size < 0)
                    continue;
                try {
                    formatter.appendRow(rowType, row, size);
                }
                catch (IOException ex) {
                    throw new AkibanInternalException("formatting error", ex);
                }
                if ((buffer.length() >= CHARS_PER_MESSAGE))
                    break;
                buffer.append('\n');
            }
            if (buffer.length() > 0) {
                String str = buffer.toString();
                buffer.setLength(0);
                messagesSent++;
                return Collections.singletonList(str);
            }
            return null;
        }

        @Override
        public void close() {
            if (cursor != null) {
                cursor.destroy();
                cursor = null;
            }
        }

        private int tableSize(UserTable table) {
            Integer size = tableSizes.get(table);
            if (size == null) {
                if (table.isDescendantOf(rootTable))
                    size = table.getColumns().size(); // Not ...IncludingInternal()...
                else
                    size = -1;
                tableSizes.put(table, size);
            }
            return size;
        }
    }

    public static abstract class GroupRowFormatter {
        protected StringBuilder buffer;
        protected String currentSchema;

        protected GroupRowFormatter(StringBuilder buffer, String currentSchema) {
            this.buffer = buffer;
            this.currentSchema = currentSchema;
        }

        public abstract void appendRow(RowType rowType, Row row, int ncols) throws IOException;
    }

    public static class SQLRowFormatter extends GroupRowFormatter {
        private Map<UserTable,String> tableNames = new HashMap<UserTable,String>();
        private SqlLiteralValueFormatter literalFormatter;

        SQLRowFormatter(StringBuilder buffer, String currentSchema) {
            super(buffer, currentSchema);
            literalFormatter = new SqlLiteralValueFormatter(buffer);
        }

        @Override
        public void appendRow(RowType rowType, Row row, int ncols) throws IOException {
            buffer.append("INSERT INTO ");
            buffer.append(tableName(rowType.userTable()));
            buffer.append(" VALUES(");
            ncols = Math.min(ncols, rowType.nFields());
            for (int i = 0; i < ncols; i++) {
                if (i > 0) buffer.append(", ");
                literalFormatter.append(row.eval(i), rowType.typeAt(i));
            }
            buffer.append(");");
        }

        protected String tableName(UserTable table) {
            String name = tableNames.get(table);
            if (name == null) {
                TableName tableName = table.getName();
                name = identifier(tableName.getTableName());
                if (!tableName.getSchemaName().equals(currentSchema)) {
                    name = identifier(tableName.getSchemaName()) + "." + name;
                }
                tableNames.put(table, name);
            }
            return name;
        }

        protected static String identifier(String name) {
            if (name.matches("[a-z][_a-z0-9]*")) // Note: lowercase only.
                return name;
            StringBuilder str = new StringBuilder();
            str.append('`');
            str.append(name.replace("`", "``"));
            str.append('`');
            return str.toString();
        }
    }

    @Override
    public int[] jdbcTypes() {
        return TYPES;
    }

    private static final int[] TYPES = new int[] { Types.VARCHAR };
}
