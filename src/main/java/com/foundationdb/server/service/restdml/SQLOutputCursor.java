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

package com.foundationdb.server.service.restdml;

import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.row.DelegateRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.service.externaldata.GenericRowTracker;
import com.foundationdb.server.service.externaldata.JsonRowWriter;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.embedded.JDBCResultSet;
import com.foundationdb.sql.embedded.JDBCResultSetMetaData;
import com.foundationdb.util.AkibanAppender;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

public class SQLOutputCursor extends GenericRowTracker implements RowCursor, JsonRowWriter.WriteRow {
    private final Deque<ResultSetHolder> holderStack = new ArrayDeque<>();
    private ResultSetHolder currentHolder;
    private FormatOptions options;

    public SQLOutputCursor(JDBCResultSet rs, FormatOptions options) throws SQLException {
        currentHolder = new ResultSetHolder(rs, null, 0);
        this.options = options;
    }

    //
    // GenericRowTracker
    //

    @Override
    public String getRowName() {
        return currentHolder.name;
    }

    //
    // Cursor
    //

    @Override
    public void open() {
    }

    @Override
    public Row next() {
        if(currentHolder == null) {
            assert holderStack.isEmpty();
            return null;
        }
        try {
            Row row = null;
            if(!holderStack.isEmpty() && holderStack.peek().depth > currentHolder.depth) {
                currentHolder = holderStack.pop();
            }
            if(currentHolder.resultSet.next()) {
                row = currentHolder.resultSet.unwrap(Row.class);
                setDepth(currentHolder.depth);
            } else if(!holderStack.isEmpty()) {
                currentHolder = holderStack.pop();
                row = next();
            } else {
                currentHolder = null;
            }
            return (row != null) ? new RowHolder(row, currentHolder) : null;
        } catch(SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        holderStack.clear();
        currentHolder = null;
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isIdle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isActive() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDestroyed() {
        throw new UnsupportedOperationException();
    }

    //
    // JsonRowWriter.WriteRow
    //

    @Override
    public void write(Row row, AkibanAppender appender, FormatOptions options) {
        if(!(row instanceof RowHolder)) {
            throw new IllegalArgumentException("Unexpected row: " + row.getClass());
        }
        try {
            RowHolder rowHolder = (RowHolder)row;
            JDBCResultSet resultSet = rowHolder.rsHolder.resultSet;
            JDBCResultSetMetaData metaData = resultSet.getMetaData();
            boolean begun = false;
            boolean savedCurrent = false;
            for(int col = 1; col <= metaData.getColumnCount(); ++col) {
                String colName = metaData.getColumnLabel(col);
                if(metaData.getNestedResultSet(col) != null) {
                    if(!savedCurrent) {
                        holderStack.push(currentHolder);
                        savedCurrent = true;
                    }
                    JDBCResultSet nested = (JDBCResultSet)resultSet.getObject(col);
                    holderStack.push(new ResultSetHolder(nested, colName, rowHolder.rsHolder.depth + 1));
                } else {
                    ValueSource valueSource = row.value(col - 1);
                    JsonRowWriter.writeValue(colName, valueSource, appender, !begun, options);
                    begun = true;
                }
            }
        } catch(SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class ResultSetHolder {
        public final JDBCResultSet resultSet;
        public final String name;
        public final int depth;

        private ResultSetHolder(JDBCResultSet resultSet, String name, int depth) {
            this.resultSet = resultSet;
            this.name = name;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return name + "(" + depth + ")";
        }
    }

    private static class RowHolder extends DelegateRow {
        public final ResultSetHolder rsHolder;

        public RowHolder(Row delegate, ResultSetHolder rsHolder) {
            super(delegate);
            this.rsHolder = rsHolder;
        }
    }
}
