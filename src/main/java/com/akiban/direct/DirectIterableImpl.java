
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
            
            boolean result = resultSet.next();
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
                Statement statement = Direct.getContext().createStatement();
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
