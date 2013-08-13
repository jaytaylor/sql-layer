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

package com.akiban.server.service.restdml;

import com.akiban.qp.operator.RowCursor;
import com.akiban.qp.row.DelegateRow;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.service.externaldata.GenericRowTracker;
import com.akiban.server.service.externaldata.JsonRowWriter;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.sql.embedded.JDBCResultSet;
import com.akiban.sql.embedded.JDBCResultSetMetaData;
import com.akiban.util.AkibanAppender;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Deque;

public class SQLOutputCursor extends GenericRowTracker implements RowCursor, JsonRowWriter.WriteRow {
    private final Deque<ResultSetHolder> holderStack = new ArrayDeque<>();
    private ResultSetHolder currentHolder;

    public SQLOutputCursor(JDBCResultSet rs) throws SQLException {
        currentHolder = new ResultSetHolder(rs, null, 0);
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
    public void write(Row row, AkibanAppender appender) {
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
                String colName = metaData.getColumnName(col);
                if(metaData.getNestedResultSet(col) != null) {
                    if(!savedCurrent) {
                        holderStack.push(currentHolder);
                        savedCurrent = true;
                    }
                    JDBCResultSet nested = (JDBCResultSet)resultSet.getObject(col);
                    holderStack.push(new ResultSetHolder(nested, colName, rowHolder.rsHolder.depth + 1));
                } else {
                    PValueSource pValueSource = row.pvalue(col - 1);
                    JsonRowWriter.writeValue(colName, pValueSource, appender, !begun);
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
