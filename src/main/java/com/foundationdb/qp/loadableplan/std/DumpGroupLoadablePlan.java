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

package com.foundationdb.qp.loadableplan.std;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.operator.BindingNotSetException;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.types.util.SqlLiteralValueFormatter;
import com.foundationdb.server.types3.Types3Switch;
import com.foundationdb.util.AkibanAppender;

import java.sql.Types;

import java.io.IOException;
import java.util.*;

/**
 * Output table contents in group order.
 */
public class DumpGroupLoadablePlan extends LoadableDirectObjectPlan
{
    @Override
    public DirectObjectPlan plan() {
        return new DirectObjectPlan() {

            @Override
            public DirectObjectCursor cursor(QueryContext context, QueryBindings bindings) {
                return new DumpGroupDirectObjectCursor(context, bindings);
            }

            @Override
            public OutputMode getOutputMode() {
                // Output as raw text, not rows.
                return OutputMode.COPY;
            }
        };
    }

    public static final int CHARS_PER_MESSAGE = 1000;
    public static final int MESSAGES_PER_FLUSH = 100;

    public static class DumpGroupDirectObjectCursor extends DirectObjectCursor {
        private final QueryContext context;
        private final QueryBindings bindings;
        private UserTable rootTable;
        private RowCursor cursor;
        private Map<UserTable,Integer> tableSizes;
        private StringBuilder buffer;
        private GroupRowFormatter formatter;
        private int messagesSent;

        public DumpGroupDirectObjectCursor(QueryContext context, QueryBindings bindings) {
            this.context = context;
            this.bindings = bindings;
        }

        @Override
        public void open() {
            String currentSchema = context.getCurrentSchema();
            String schemaName, tableName;
            if (Types3Switch.ON) {
                if (bindings.getPValue(0).isNull())
                    schemaName = currentSchema;
                else
                    schemaName = bindings.getPValue(0).getString();
                tableName = bindings.getPValue(1).getString();
            }
            else {
                if (bindings.getValue(0).isNull())
                    schemaName = currentSchema;
                else
                    schemaName = bindings.getValue(0).getString();
                tableName = bindings.getValue(1).getString();
            }
            rootTable = context.getStore().schema().ais()
                .getUserTable(schemaName, tableName);
            if (rootTable == null)
                throw new NoSuchTableException(schemaName, tableName);
            cursor = context.getStore(rootTable)
                .newGroupCursor(rootTable.getGroup());
            cursor.open();
            tableSizes = new HashMap<>();
            buffer = new StringBuilder();
            int insertMaxRowCount;
            try {
                if (Types3Switch.ON) {
                    insertMaxRowCount = bindings.getPValue(2).getInt32();
                }
                else {
                    insertMaxRowCount = (int)bindings.getValue(2).getLong();
                }
            }
            catch (BindingNotSetException ex) {
                insertMaxRowCount = 1;
            }
            formatter = new SQLRowFormatter(buffer, currentSchema, insertMaxRowCount);
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
            }
            formatter.flush();
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
        public void flush() {
        }
    }

    public static class SQLRowFormatter extends GroupRowFormatter {
        private Map<UserTable,String> tableNames = new HashMap<>();
        private int maxRowCount;
        private SqlLiteralValueFormatter literalFormatter;
        private AkibanAppender appender;
        private RowType lastRowType;
        private int rowCount, insertWidth;

        SQLRowFormatter(StringBuilder buffer, String currentSchema, int maxRowCount) {
            super(buffer, currentSchema);
            this.maxRowCount = maxRowCount;
            if (Types3Switch.ON) {
                appender = AkibanAppender.of(buffer);
            }
            else {
                literalFormatter = new SqlLiteralValueFormatter(buffer);
                // TODO: Workaround INSERT problems with literal timestamp into datetime field.
                literalFormatter.setDateTimeFormat(SqlLiteralValueFormatter.DateTimeFormat.NONE);
            }
        }

        @Override
        public void appendRow(RowType rowType, Row row, int ncols) throws IOException {
            if ((lastRowType == rowType) &&
                (rowCount++ < maxRowCount)) {
                buffer.append(",\n");
                for (int i = 0; i < insertWidth; i++) {
                    buffer.append(' ');
                }
            }
            else {
                flush();
                int pos = buffer.length();
                buffer.append("INSERT INTO ");
                buffer.append(tableName(rowType.userTable()));
                buffer.append(" VALUES");
                insertWidth = buffer.length() - pos;
                lastRowType = rowType;
                rowCount = 1;
            }
            buffer.append('(');
            ncols = Math.min(ncols, rowType.nFields());
            for (int i = 0; i < ncols; i++) {
                if (i > 0) buffer.append(", ");
                if (Types3Switch.ON) {
                    rowType.typeInstanceAt(i).formatAsLiteral(row.pvalue(i), appender);
                }
                else {
                    literalFormatter.append(row.eval(i), rowType.typeAt(i));
                }
            }
            buffer.append(')');
        }

        public void flush() {
            if (rowCount > 0) {
                buffer.append(";\n");
                lastRowType = null;
                rowCount = 0;
            }
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
