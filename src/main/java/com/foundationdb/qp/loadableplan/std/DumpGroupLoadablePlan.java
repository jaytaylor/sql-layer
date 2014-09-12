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

package com.foundationdb.qp.loadableplan.std;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.loadableplan.DirectObjectCursor;
import com.foundationdb.qp.loadableplan.DirectObjectPlan;
import com.foundationdb.qp.loadableplan.LoadableDirectObjectPlan;
import com.foundationdb.qp.operator.BindingNotSetException;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.server.ServerTransaction;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;

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
            public TransactionMode getTransactionMode() {
                return TransactionMode.READ_ONLY;
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
        private Table rootTable;
        private RowCursor cursor;
        private Map<Table,Integer> tableSizes;
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
            ValueSource value = valueNotNull(0);
            if (value == null)
                schemaName = currentSchema;
            else
                schemaName = value.getString();
            tableName = bindings.getValue(1).getString();
            rootTable = context.getStore().schema().ais()
                .getTable(schemaName, tableName);
            if (rootTable == null)
                throw new NoSuchTableException(schemaName, tableName);
            int commitFrequency;
            value = valueNotNull(3);
            if (value != null)
                commitFrequency = value.getInt32();
            else if (context.getTransactionPeriodicallyCommit() != ServerTransaction.PeriodicallyCommit.OFF)
                commitFrequency = StoreAdapter.COMMIT_FREQUENCY_PERIODICALLY;
            else
                commitFrequency = 0;
            cursor = context.getStore(rootTable)
                .newDumpGroupCursor(rootTable.getGroup(), commitFrequency);
            cursor.open();
            tableSizes = new HashMap<>();
            buffer = new StringBuilder();
            int insertMaxRowCount;
            value = valueNotNull(2);
            if (value == null)
                insertMaxRowCount = 1;
            else
                insertMaxRowCount = value.getInt32();
            formatter = new SQLRowFormatter(buffer, currentSchema, insertMaxRowCount);
            messagesSent = 0;
        }

        protected ValueSource valueNotNull(int index) {
            try {
                ValueSource value = bindings.getValue(index);
                if (value.isNull())
                    return null;
                else
                    return value;
            }
            catch (BindingNotSetException ex) {
                return null;
            }            
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
                Table rowTable = rowType.table();
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
                cursor.close();
                cursor = null;
            }
        }

        private int tableSize(Table table) {
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
        private Map<Table,String> tableNames = new HashMap<>();
        private int maxRowCount;
        private AkibanAppender appender;
        private RowType lastRowType;
        private int rowCount, insertWidth;

        SQLRowFormatter(StringBuilder buffer, String currentSchema, int maxRowCount) {
            super(buffer, currentSchema);
            this.maxRowCount = maxRowCount;
            appender = AkibanAppender.of(buffer);
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
                buffer.append(tableName(rowType.table()));
                buffer.append(" VALUES");
                insertWidth = buffer.length() - pos;
                lastRowType = rowType;
                rowCount = 1;
            }
            buffer.append('(');
            ncols = Math.min(ncols, rowType.nFields());
            for (int i = 0; i < ncols; i++) {
                if (i > 0) buffer.append(", ");
                rowType.typeAt(i).formatAsLiteral(row.value(i), appender);
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

        protected String tableName(Table table) {
            String name = tableNames.get(table);
            if (name == null) {
                TableName tableName = table.getName();
                name = Strings.quotedIdent(tableName.getTableName(), '`', false);
                if (!tableName.getSchemaName().equals(currentSchema)) {
                    name = Strings.quotedIdent(tableName.getSchemaName(), '`', false) + "." + name;
                }
                tableNames.put(table, name);
            }
            return name;
        }
    }

    @Override
    public int[] jdbcTypes() {
        return TYPES;
    }

    private static final int[] TYPES = new int[] { Types.VARCHAR };
}
