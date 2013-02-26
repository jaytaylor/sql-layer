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

    final String toTable;
    final String fromTable;

    final List<String> predicates = new ArrayList<String>();
    String sort;
    String limit;

    boolean initialized;
    String sql;

    DirectResultSet resultSet;

    public DirectIterableImpl(Class<T> clazz, String fromTable, String toTable) {
        this.clazz = clazz;
        this.fromTable = fromTable;
        this.toTable = toTable;
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
        if (hasNext) {
            try {
                T result = (T) resultSet.getEntity(clazz);
                if (nextRow()) {
                    throw new IllegalStateException("Expected only one row");
                }
                return result;
            } catch (SQLException e) {
                throw new DirectException(e);
            }
        }
        return null;
    }

    private boolean nextRow() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new DirectException(e);
        }
    }

    private void initIfNeeded() {
        if (!initialized) {
            StringBuilder sb = new StringBuilder();
            sb.append("select ").append(toTable).append(".*").append(" from ").append(toTable);
            if (fromTable != null) {
                sb.append(", ").append(fromTable);
            }
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
            if (sort != null) {
                sb.append(" order by ").append(sort);
            }
            if (limit != null) {
                sb.append(" limit ").append(limit);
            }

            try {
                Statement statement = Direct.getDirectContext().createStatement();
                sql = sb.toString();
                resultSet = (DirectResultSet) statement.executeQuery(sql);
            } catch (SQLException e) {
                throw new DirectException(e);
            }
            initialized = true;
        }
    }

    /**
     * TODO: this implementation assumes a predicate in which the first element
     * is a column name in the child table. This method prepends that with the
     * child table name. E.g.
     * 
     * <pre>
     * <code>
     * customer.getOrderList.where("order_date > '2010-01-01'") 
     * </code>
     * </pre>
     * 
     * is translated to a clause like
     * 
     * <pre>
     * <code>
     * where orders.orderDate > '2010-01-01'
     * </code>
     * </pre>
     * 
     * 
     */
    @Override
    public DirectIterableImpl<T> where(final String predicate) {
        predicates.add(toTable + "." + predicate);
        return this;
    }

    /**
     * TODO: this implementation assumes a string in which the first element is
     * a column name in the child table. E.g.,
     * 
     * <pre>
     * <code>
     * customer.getOrderList.sort("order_date desc") 
     * </code>
     * </pre>
     * 
     * is translated to a clause like
     * 
     * <pre>
     * <code>
     * order by orders.orderDate desc
     * </code>
     * </pre>
     * 
     * 
     */
    @Override
    public DirectIterableImpl<T> sort(final String sort) {
        if (this.sort == null) {
            this.sort = sort;
            return this;
        }
        throw new IllegalStateException("Sort already specified");
    }

    public DirectIterableImpl<T> naturalJoin(final String fromColumn, String toColumn) {
        return where(toColumn + " = " + fromTable + "." + fromColumn);
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
