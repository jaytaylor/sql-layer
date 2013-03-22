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

package com.akiban.sql.embedded;

import com.akiban.ais.model.Parameter;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ColumnBinding;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.types.DataTypeDescriptor;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import java.util.List;

public class JDBCParameterMetaData implements ParameterMetaData
{
    protected static class ParameterType {
        private String name;    // null for ordinary (non-CALL) prepared statements
        private DataTypeDescriptor sqlType;
        private int jdbcType;
        private AkType akType;
        private TInstance tInstance;
        private int mode;       // parameterModeXxx (In for non-CALL)

        protected static DataTypeDescriptor getType(Parameter param) {
            try {
                return ColumnBinding.getType(param);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
        }

        protected ParameterType(Parameter param) {
            this(getType(param));
            this.name = param.getName();
            switch (param.getDirection()) {
            case IN:
                mode = parameterModeIn;
                break;
            case OUT:
            case RETURN:
                mode = parameterModeOut;
                break;
            case INOUT:
                mode = parameterModeInOut;
                break;
            }
        }

        protected ParameterType(DataTypeDescriptor sqlType) {
            this.sqlType = sqlType;
            if (sqlType != null) {
                jdbcType = sqlType.getJDBCTypeId();
                akType = TypesTranslation.sqlTypeToAkType(sqlType);
                tInstance = TypesTranslation.toTInstance(sqlType);
            }
            mode = parameterModeIn;
        }

        public String getName() {
            return name;
        }

        public DataTypeDescriptor getSQLType() {
            return sqlType;
        }

        public int getJDBCType() {
            return jdbcType;
        }

        public AkType getAkType() {
            return akType;
        }

        public TInstance getTInstance() {
            return tInstance;
        }

        public int getScale() {
            return sqlType.getScale();
        }

        public int getPrecision() {
            return sqlType.getPrecision();
        }

        public boolean isNullable() {
            return sqlType.isNullable();
        }

        public String getTypeName() {
            return sqlType.getTypeName();
        }

        public int getMode() {
            return mode;
        }
    }
    
    private List<ParameterType> params;

    protected JDBCParameterMetaData(List<ParameterType> params) {
        this.params = params;
    }

    protected List<ParameterType> getParameters() {
        return params;
    }

    protected ParameterType getParameter(int param) {
        return params.get(param - 1);
    }

    public String getParameterName(int param) throws SQLException {
        return getParameter(param).getName();
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

    /* ParameterMetaData */

    @Override
    public int getParameterCount() throws SQLException {
        return params.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return getParameter(param).isNullable() ? parameterNullable : parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return JDBCResultSetMetaData.isTypeSigned(getParameter(param).getAkType());
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return getParameter(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return getParameter(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return getParameter(param).getJDBCType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getParameter(param).getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return JDBCResultSetMetaData.getTypeClassName(getParameter(param).getAkType());
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return getParameter(param).getMode();
    }
}
