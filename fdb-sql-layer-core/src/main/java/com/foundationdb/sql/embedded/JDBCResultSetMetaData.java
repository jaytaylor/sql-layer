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

package com.foundationdb.sql.embedded;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.util.List;

public class JDBCResultSetMetaData implements ResultSetMetaData
{
    protected static class ResultColumn extends PhysicalResultColumn {
        private int jdbcType;
        private DataTypeDescriptor sqlType;
        private Column aisColumn;
        private TInstance type;
        private JDBCResultSetMetaData nestedResultSet;

        protected ResultColumn(String name, 
                               int jdbcType, DataTypeDescriptor sqlType, 
                               Column aisColumn, TInstance type,
                               JDBCResultSetMetaData nestedResultSet) {
            super(name);
            this.jdbcType = jdbcType;
            this.sqlType = sqlType;
            this.aisColumn = aisColumn;
            this.type = type;
            this.nestedResultSet = nestedResultSet;
        }

        public int getJDBCType() {
            return jdbcType;
        }

        public DataTypeDescriptor getSQLType() {
            return sqlType;
        }

        public Column getAISColumn() {
            return aisColumn;
        }
        
        public TInstance getType() {
            return type;
        }

        public int getScale() {
            if (sqlType != null)
                return sqlType.getScale();
            if ((aisColumn != null) && (aisColumn.getTypeParameter1() != null))
                return aisColumn.getTypeParameter1().intValue();
            return 0;
        }

        public int getPrecision() {
            if (sqlType != null)
                return sqlType.getPrecision();
            if ((aisColumn != null) && (aisColumn.getTypeParameter2() != null))
                return aisColumn.getTypeParameter2().intValue();
            return 0;
        }

        public boolean isNullable() {
            if (sqlType != null)
                return sqlType.isNullable();
            if (aisColumn != null)
                return (aisColumn.getNullable() == Boolean.TRUE);
            return false;
        }

        public String getTypeName() {
            if (sqlType != null)
                return sqlType.getTypeName();
            if (aisColumn != null)
                return aisColumn.getTypeName();
            return "";
        }

        public int getMaximumWidth() {
            if (sqlType != null)
                return sqlType.getMaximumWidth();
            return 1024;
        }

        public JDBCResultSetMetaData getNestedResultSet() {
            return nestedResultSet;
        }
    }

    private final TypesTranslator typesTranslator;
    private final List<ResultColumn> columns;

    protected JDBCResultSetMetaData(TypesTranslator typesTranslator,
                                    List<ResultColumn> columns) {
        this.typesTranslator = typesTranslator;
        this.columns = columns;
    }

    protected List<ResultColumn> getColumns() {
        return columns;
    }

    protected ResultColumn getColumn(int column) {
        return columns.get(column - 1);
    }

    /* Wrapper */

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
    
    /* ResultSetMetaData */

    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        // Not supported any longer, so no 
        return false; 
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        Column aisColumn = getColumn(column).getAISColumn();
        if (aisColumn == null)
            return false;
        AkCollator collator = aisColumn.getCollator();
        if (collator == null)
            return false;
        else
            return collator.isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumn(column).isNullable() ? columnNullable : columnNoNulls;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return typesTranslator.isTypeSigned(getColumn(column).getType());
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumn(column).getMaximumWidth();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).getName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        ResultColumn jdbcColumn = getColumn(column);
        Column aisColumn = jdbcColumn.getAISColumn();
        if (aisColumn != null)
            return aisColumn.getName();
        return jdbcColumn.getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        Column aisColumn = getColumn(column).getAISColumn();
        if (aisColumn != null)
            return aisColumn.getTable().getName().getSchemaName();
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        Column aisColumn = getColumn(column).getAISColumn();
        if (aisColumn != null)
            return aisColumn.getTable().getName().getTableName();
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).getJDBCType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return typesTranslator.jdbcClass(getColumn(column).getType()).getName();
    }


    public JDBCResultSetMetaData getNestedResultSet(int column) {
        return getColumn(column).getNestedResultSet();
    }
}
