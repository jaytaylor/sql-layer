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

package com.akiban.server.service.restdml;

import com.akiban.qp.operator.Cursor;
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

public class EmbeddedSQLOutputHelper extends GenericRowTracker implements Cursor, JsonRowWriter.WriteRow {
    private final Deque<ResultSetHolder> holderStack = new ArrayDeque<>();
    private ResultSetHolder holder;

    public EmbeddedSQLOutputHelper(JDBCResultSet rs) throws SQLException {
        holder = new ResultSetHolder(rs, null, 0);
    }

    // GenericRowTracker

    @Override
    public String getRowName() {
        return holder.name;
    }

    // Cursor

    @Override
    public void open() {
        // None
    }

    @Override
    public Row next() {
        if(holder == null) {
            assert holderStack.isEmpty();
            return null;
        }
        try {
            Row row = null;
            if(!holderStack.isEmpty() && holderStack.peek().depth > holder.depth) {
                holder = holderStack.pop();
            }
            if(holder.rs.next()) {
                row = holder.rs.unwrap(Row.class);
                setDepth(holder.depth);
            } else if(!holderStack.isEmpty()) {
                holder = holderStack.pop();
                row = next();
            } else {
                holder = null;
            }
            return row;
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
        // None
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

    // JsonRowWriter.WriteRow

    @Override
    public void write(Row row, AkibanAppender appender) {
        try {
            JDBCResultSetMetaData metaData = holder.metaData();
            boolean savedCurrent = false;
            int count = metaData.getColumnCount();
            for(int i = 0; i < count; ++i) {
                String name = metaData.getColumnName(i+1);
                if(metaData.getNestedResultSet(i+1) != null) {
                    if(!savedCurrent) {
                        holderStack.push(holder);
                        savedCurrent = true;
                    }
                    JDBCResultSet nested = (JDBCResultSet)holder.rs.getObject(i+1);
                    holderStack.push(new ResultSetHolder(nested, name, holder.depth + 1));
                } else {
                    PValueSource pValueSource = row.pvalue(i);
                    JsonRowWriter.writeValue(name, pValueSource, appender, i == 0);
                }
            }
        } catch(SQLException e) {
            throw new IllegalStateException(e);
        }
    }


    private static class ResultSetHolder {
        public final JDBCResultSet rs;
        public final String name;
        public final int depth;

        private ResultSetHolder(JDBCResultSet rs, String name, int depth) {
            this.rs = rs;
            this.name = name;
            this.depth = depth;
        }

        @Override
        public String toString() {
            return name + "(" + depth + ")";
        }

        public JDBCResultSetMetaData metaData() {
            try {
                return rs.getMetaData();
            } catch(SQLException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

}
