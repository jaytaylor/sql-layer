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

package com.akiban.sql.embedded;

import com.akiban.ais.model.Column;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.types.DataTypeDescriptor;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.util.List;

public class JDBCResultSetMetaData implements ResultSetMetaData
{
    protected static class JDBCResultColumn extends PhysicalResultColumn {
        private int jdbcType;
        private DataTypeDescriptor sqlType;
        private Column aisColumn;
        private TInstance tInstance;

        public JDBCResultColumn(String name, int jdbcType, DataTypeDescriptor sqlType, 
                                Column aisColumn, TInstance tInstance) {
            super(name);
            this.jdbcType = jdbcType;
            this.sqlType = sqlType;
            this.tInstance = tInstance;
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
        
        public TInstance getTInstance() {
            return tInstance;
        }

        public AkType getAkType() {
            if (aisColumn != null)
                return aisColumn.getType().akType();
            if (sqlType != null)
                return TypesTranslation.sqlTypeToAkType(sqlType);
            return AkType.UNSUPPORTED;
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
                return aisColumn.getType().name();
            return "";
        }

        public int getMaximumWidth() {
            if (sqlType != null)
                return sqlType.getMaximumWidth();
            return 1024;
        }
    }

    private List<JDBCResultColumn> columns;

    protected JDBCResultSetMetaData(List<JDBCResultColumn> columns) {
        this.columns = columns;
    }

    protected JDBCResultColumn getColumn(int column) {
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
        Column aisColumn = getColumn(column).getAISColumn();
        if (aisColumn == null)
            return false;
        else
            // No isAutoIncrement().
            return (aisColumn.getInitialAutoIncrementValue() != null);
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
        switch (getColumn(column).getAkType()) {
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case INT:
        case LONG:
            return true;
        default:
            return false;
        }
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
        JDBCResultColumn jdbcColumn = getColumn(column);
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
        switch (getColumn(column).getAkType()) {
        case DATE:
            return "java.sql.Date";
        case TIMESTAMP:
        case DATETIME:
            return "java.sql.Timestamp";
        case DECIMAL:
            return "java.math.BigDecimal";
        case DOUBLE:
        case U_DOUBLE:
            return "java.lang.Double";
        case FLOAT:
        case U_FLOAT:
            return "java.lang.Float";
        case INT:
        case YEAR:
            return "java.lang.Integer";
        case LONG:
        case U_INT:
            return "java.lang.Long";
        case VARCHAR:
        case TEXT:
            return "java.lang.String";
        case TIME:
            return "java.sql.Time";
        case U_BIGINT:
            return "java.math.BigInteger";
        case VARBINARY:
            return "java.lang.byte[]";
        case BOOL:
            return "java.lang.Boolean";
        case RESULT_SET:
            return JDBCResultSet.class.getName();
        default:
            return "java.lang.Object";
        }
    }
}
