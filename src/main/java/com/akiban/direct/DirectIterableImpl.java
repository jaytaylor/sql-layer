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

package com.akiban.direct;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.akiban.qp.row.Row;
import com.akiban.sql.embedded.JDBCResultSet;
import com.akiban.util.ShareHolder;

/**
 * Very kludgey implementation by constructing SQL strings.
 * 
 * @author peter
 * 
 * @param <T>
 */
public class DirectIterableImpl<T> implements DirectIterable<T> {

    final Class<T> clazz;
    boolean hasNext;

    final String table;
    final List<String> predicates = new ArrayList<String>();
    final List<String> sorts = new ArrayList<String>();
    String limit;

    boolean initialized;
    String sql;

    JDBCResultSet resultSet;
    
    ShareHolder<Row> rowHolder = new ShareHolder<Row>();

    public DirectIterableImpl(Class<T> clazz, String toTable) {
        this.clazz = clazz;
        this.table = toTable;
    }

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {

            @Override
            public boolean hasNext() {
                initIfNeeded();
                hasNext = nextRow();
                return hasNext;
            }

            @SuppressWarnings("unchecked")
            @Override
            public T next() {
                initIfNeeded();
                if (!hasNext) {
                    return null;
                }
                T result;
                try {
                    result = (T) resultSet.getEntity(clazz);
                } catch (SQLException e) {
                    throw new DirectException(e);
                }
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Can't remove yet");
            }
        };
    }

    @SuppressWarnings("unchecked")
    public T single() {
        initIfNeeded();
        if (nextRow()) {
            try {
                T result = (T) resultSet.getEntity(clazz);
                return result;
            } catch (SQLException e) {
                throw new DirectException(e);
            }
        }
        return null;
    }

    private boolean nextRow() {
        try {
            if (rowHolder.isHolding()) {
                rowHolder.release();
            }
            boolean result = resultSet.next();
            if (result) {
                rowHolder.hold(resultSet.currentRow());
            }
            return result;
        } catch (SQLException e) {
            throw new DirectException(e);
        }
    }

    private void initIfNeeded() {
        if (!initialized) {
            StringBuilder sb = new StringBuilder();
            sb.append("select * from ").append(table);
            if (!predicates.isEmpty()) {
                sb.append(" where ");
                boolean and = false;
                for (String p : predicates) {
                    if (and) {
                        sb.append(" and ");
                    }
                    sb.append(p);
                    and = true;
                }
            }
            boolean orderBy = false;
            for (final String sort : sorts) {
                sb.append(orderBy ? ", " : " order by ").append(sort);
                orderBy = true;
            }
            if (limit != null) {
                sb.append(" limit ").append(limit);
            }

            try {
                Statement statement = Direct.getDirectContext().createStatement();
                sql = sb.toString();
                resultSet = (JDBCResultSet) statement.executeQuery(sql);
            } catch (SQLException e) {
                throw new DirectException(e);
            }
            initialized = true;
        }
    }

    @Override
    public DirectIterableImpl<T> where(final String predicate) {
        predicates.add(predicate);
        return this;
    }
    
    @Override
    public DirectIterableImpl<T> where(final String columnName, Object literal) {
        StringBuilder sb = new StringBuilder(columnName).append(" = ");
        if (literal instanceof Number) {
            sb.append(literal);
        } else {
            sb.append('\'').append(literal.toString()).append('\'');
        }
        predicates.add(sb.toString());
        return this;
    }
    

    @Override
    public DirectIterableImpl<T> sort(final String column) {
        sorts.add(column);
        return this;
    }

    @Override
    public DirectIterableImpl<T> sort(final String column, String direction) {
        sorts.add(column + " " + direction);
        return this;
    }

    @Override
    public DirectIterableImpl<T> limit(final String limit) {
        if (this.limit == null) {
            this.limit = limit;
            return this;
        }
        throw new IllegalStateException("Limit already specified");
    }
}
