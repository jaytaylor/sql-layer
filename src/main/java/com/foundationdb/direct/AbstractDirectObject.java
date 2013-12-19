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

package com.foundationdb.direct;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

public abstract class AbstractDirectObject implements DirectObject {

    private final static Map<Connection, Map<BitSet, PreparedStatement>> updateStatementCache = new WeakHashMap<>();
    private final static Map<Connection, Map<BitSet, PreparedStatement>> insertStatementCache = new WeakHashMap<>();

    /*
     * 1. schema_name 2. tableName 3. comma-separated list of column names, 4.
     * comma-separated list of '?' symbols.
     */
    private final static String INSERT_STATEMENT = "insert into \"%s\".\"%s\" (%s) values (%s) returning *";

    /*
     * 1. schema name 2. table name 3. comma-separated list of column_name=?
     * pairs 4. predicate: pkcolumn=?, ...
     */
    private final static String UPDATE_STATEMENT = "update \"%s\".\"%s\" set %s where %s";

    private final static Object NOT_SET = new Object() {
        @Override
        public String toString() {
            return "NOT_SET";
        }
    };

    private static Column[] columns;
    private static String schemaName;
    private static String tableName;

    protected static class Column implements DirectColumn {

        final int columnIndex;
        final String columnName;
        final String propertyName;
        final String propertyType;
        final int primaryKeyFieldIndex;
        final int parentJoinFieldIndex;

        protected Column(final int columnIndex, final String columnName, final String propertyName,
                final String propertyType, final int primaryKeyFieldIndex, final int parentJoinFieldIndex) {
            this.columnIndex = columnIndex;
            this.columnName = columnName;
            this.propertyName = propertyName;
            this.propertyType = propertyType;
            this.primaryKeyFieldIndex = primaryKeyFieldIndex;
            this.parentJoinFieldIndex = parentJoinFieldIndex;
        }

        public int getColumnIndex() {
            return columnIndex;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public String getPropertyType() {
            return propertyType;
        }
    }

    /**
     * Static initializer of subclass passes a string declaring the columns.
     * Format is columnName:propertyName:propertyType:primaryKeyFieldIndex:
     * parentjoinField,...
     * 
     * @param columnSpecs
     */
    protected static void __init(final String sName, final String tName, final String columnSpecs) {
        try {
            schemaName = sName;
            tableName = tName;
            String[] columnArray = columnSpecs.split(",");
            columns = new Column[columnArray.length];
            for (int index = 0; index < columnArray.length; index++) {
                String[] v = columnArray[index].split(":");
                columns[index] = new Column(index, v[0], v[1], v[2], Integer.parseInt(v[3]), Integer.parseInt(v[4]));
            }
        } catch (Exception e) {
            throw new DirectException(e);
        }
    }

    private Object[] updates;
    private DirectResultSet rs;

    public void setResults(DirectResultSet rs) {
        if (updates != null) {
            throw new IllegalStateException("Updates not saved: " + updates);
        }
        this.rs = rs;
        updates = null;
    }

    private Object[] updates() {
        if (updates == null) {
            updates = new Object[columns.length];
            Arrays.fill(updates, NOT_SET);
        }
        return updates;
    }

    /**
     * Instantiates (update) values for parent join fields. This method is
     * invoked from {@link DirectIterableImpl#newInstance()}.
     * 
     * @param parent
     */
    void populateJoinFields(DirectObject parent) {
        if (parent instanceof AbstractDirectObject) {
            AbstractDirectObject ado = (AbstractDirectObject) parent;
            if (parent != null) {
                for (int index = 0; index < columns.length; index++) {
                    Column c = columns[index];
                    if (c.parentJoinFieldIndex >= 0) {
                        updates()[index] = ado.__getObject(c.parentJoinFieldIndex);
                    }
                }
            }
        }
    }

    protected boolean __getBoolean(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getBoolean(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (boolean) updates[p];
        }
    }

    protected void __setBoolean(int p, boolean v) {
        updates()[p] = v;
    }

    protected Date __getDate(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getDate(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (Date) updates[p];
        }
    }

    protected void __setDate(int p, Date v) {
        updates()[p] = v;
    }

    protected BigDecimal __getBigDecimal(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getBigDecimal(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (BigDecimal) updates[p];
        }
    }

    protected void __setBigDecimal(int p, BigDecimal v) {
        updates()[p] = v;
    }

    protected byte __getByte(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getByte(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (byte) updates[p];
        }
    }

    protected void __setByte(int p, byte v) {
        updates()[p] = v;
    }

    protected byte[] __getBytes(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getBytes(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (byte[]) updates[p];
        }
    }

    protected void __setBytes(int p, byte[] v) {
        updates()[p] = v;
    }

    protected double __getDouble(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getDouble(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (double) updates[p];
        }
    }

    protected void __setDouble(int p, double v) {
        updates()[p] = v;
    }

    protected float __getFloat(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getFloat(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (float) updates[p];
        }
    }

    protected void __setFloat(int p, float v) {
        updates()[p] = v;
    }

    protected int __getInt(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getInt(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (int) updates[p];
        }
    }

    protected void __setInt(int p, int v) {
        updates()[p] = v;
    }

    protected long __getLong(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getLong(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (long) updates[p];
        }
    }

    protected void __setLong(int p, long v) {
        updates()[p] = v;
    }

    protected short __getShort(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getShort(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (short) updates[p];
        }
    }

    protected void __setShort(int p, short v) {
        updates()[p] = v;
    }

    protected String __getString(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getString(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (String) updates[p];
        }
    }

    protected void __setString(int p, String v) {
        updates()[p] = v;
    }

    protected Time __getTime(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getTime(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (Time) updates[p];
        }
    }

    protected void __setTime(int p, Time v) {
        updates()[p] = v;
    }

    protected Timestamp __getTimestamp(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getTimestamp(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return (Timestamp) updates[p];
        }
    }

    protected void __setTimestamp(int p, Timestamp v) {
        updates()[p] = v;
    }

    protected Object __getObject(int p) {
        if (updates == null || updates[p] == NOT_SET) {
            try {
                return rs.getObject(p + 1);
            } catch (SQLException e) {
                throw (RuntimeException)e.getCause();
            }
        } else {
            return updates[p];
        }
    }

    protected void __setObject(int p, Object v) {
        updates()[p] = v;
    }

    /**
     * Issue either an INSERT or an UPDATE statement depending on whether this
     * instance is bound to a result set.
     */
    public void save() {
        try {
            /*
             * If rs == null then this instance was created via the
             * DirectIterable#newInstance method and the intention is to INSERT
             * it. If rs is not null, then this instance was selected from an
             * existing table and the intention is to UPDATE it.
             */
            if (rs == null) {
                PreparedStatement stmt = __insertStatement();
                stmt.execute();
                rs = (DirectResultSet)stmt.getGeneratedKeys();
                try {
                    rs.next();
                } catch (SQLException e) {
                    throw new DirectException(e);
                }
                updates = null;
            } else {
                PreparedStatement stmt = __updateStatement();
                stmt.execute();
                updates = null;
            }

        } catch (SQLException e) {
            throw new DirectException(e);
        }
    }

    private PreparedStatement __insertStatement() throws SQLException {
        assert updates != null : "No updates to save";
        BitSet bs = new BitSet(columns.length);
        for (int index = 0; index < updates.length; index++) {
            if (updates[index] != NOT_SET) {
                bs.set(index);
            }
        }
        Connection conn = Direct.getContext().getConnection();
        Map<BitSet, PreparedStatement> map = insertStatementCache.get(conn);
        PreparedStatement stmt = null;
        if (map == null) {
            map = new HashMap<>();
            insertStatementCache.put(conn, map);
        } else {
            stmt = map.get(bs);
        }
        if (stmt == null) {
            StringBuilder updateColumns = new StringBuilder();
            StringBuilder updateValues = new StringBuilder();

            for (int index = 0; index < columns.length; index++) {
                if (updates[index] != NOT_SET) {
                    if (updateColumns.length() > 0) {
                        updateColumns.append(',');
                        updateValues.append(',');
                    }
                    updateColumns.append('\"').append(columns[index].columnName).append('\"');
                    updateValues.append('?');
                }
            }
            final String sql = String.format(INSERT_STATEMENT, schemaName, tableName, updateColumns, updateValues);
            stmt = conn.prepareStatement(sql);
            map.put(bs, stmt);
        } else {
            // Just in case
            stmt.clearParameters();
        }
        int statementIndex = 1;
        for (int index = 0; index < columns.length; index++) {
            if (updates[index] != NOT_SET) {
                stmt.setObject(statementIndex, updates[index]);
                statementIndex++;
            }
        }
        return stmt;
    }

    private PreparedStatement __updateStatement() throws SQLException {
        assert updates != null : "No updates to save";
        BitSet bs = new BitSet(columns.length);
        for (int index = 0; index < updates.length; index++) {
            if (updates[index] != NOT_SET) {
                bs.set(index);
            }
        }
        Connection conn = Direct.getContext().getConnection();

        Map<BitSet, PreparedStatement> map = updateStatementCache.get(conn);
        synchronized (this) {
            if (map == null) {
                map = new HashMap<>();
                updateStatementCache.put(conn, map);
            }
        }

        PreparedStatement stmt = map.get(bs);
        if (stmt == null) {
            StringBuilder updateColumns = new StringBuilder();
            StringBuilder pkColumns = new StringBuilder();

            for (int index = 0; index < columns.length; index++) {
                if (columns[index].parentJoinFieldIndex >= 0 || columns[index].primaryKeyFieldIndex >= 0) {
                    if (pkColumns.length() > 0) {
                        pkColumns.append(" and ");
                    }
                    pkColumns.append(columns[index].columnName).append("=?");
                }
                if (updates[index] != NOT_SET) {
                    if (updateColumns.length() > 0) {
                        updateColumns.append(',');
                    }
                    updateColumns.append(columns[index].getColumnName()).append("=?");
                }
            }
            final String sql = String.format(UPDATE_STATEMENT, schemaName, tableName, updateColumns, pkColumns);
            stmt = conn.prepareStatement(sql);
            map.put(bs, stmt);
        } else {
            // Just in case
            stmt.clearParameters();
        }
        int statementIndex = 1;
        for (int pass = 0; pass < 2; pass++) {
            for (int index = 0; index < columns.length; index++) {
                if (pass == 0) {
                    if (updates[index] != NOT_SET) {
                        stmt.setObject(statementIndex, updates[index]);
                        statementIndex++;
                    }
                }
                if (pass == 1) {
                    if (columns[index].parentJoinFieldIndex >= 0 || columns[index].primaryKeyFieldIndex >= 0) {
                        stmt.setObject(statementIndex, __getObject(index));
                        statementIndex++;
                    }
                }
            }
        }
        return stmt;
    }

}
