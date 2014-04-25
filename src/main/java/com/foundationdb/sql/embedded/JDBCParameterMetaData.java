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

import com.foundationdb.ais.model.Parameter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import java.util.List;

public class JDBCParameterMetaData implements ParameterMetaData
{
    protected static class ParameterType {
        private String name;    // null for ordinary (non-CALL) prepared statements
        private DataTypeDescriptor sqlType;
        private int jdbcType;
        private TInstance type;
        private int mode;       // parameterModeXxx (In for non-CALL)

        protected ParameterType(Parameter param, DataTypeDescriptor sqlType,
                                int jdbcType, TInstance type) {
            this.sqlType = sqlType;
            this.jdbcType = jdbcType;
            this.type = type;

            name = param.getName();
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

        protected ParameterType(DataTypeDescriptor sqlType,
                                int jdbcType, TInstance type) {
            this.sqlType = sqlType;
            this.jdbcType = jdbcType;
            this.type = type;

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

        public TInstance getType() {
            return type;
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
    
    private final TypesTranslator typesTranslator;
    private final List<ParameterType> params;

    protected JDBCParameterMetaData(TypesTranslator typesTranslator,
                                    List<ParameterType> params) {
        this.typesTranslator = typesTranslator;
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
        return typesTranslator.isTypeSigned(getParameter(param).getType());
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
        return typesTranslator.jdbcClass(getParameter(param).getType()).getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return getParameter(param).getMode();
    }
}
