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

package com.foundationdb.sql.pg;

import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.sql.optimizer.NestedResultSetTypeComputer;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.foundationdb.sql.optimizer.plan.PhysicalSelect;
import com.foundationdb.sql.optimizer.plan.PhysicalUpdate;
import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.TInstance;

import java.util.*;

public class PostgresJsonCompiler extends PostgresOperatorCompiler
{
    protected PostgresJsonCompiler() {
    }

    @Override
    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setAllowSubqueryMultipleColumns(true);
    }

    @Override
    protected void initTypesRegistry(TypesRegistryService typesRegistry) {
        super.initTypesRegistry(typesRegistry);
        typeComputer = new NestedResultSetTypeComputer(typesRegistry);
    }

    public static PostgresJsonCompiler create(PostgresServerSession server, KeyCreator keyCreator) {
        PostgresJsonCompiler compiler = new PostgresJsonCompiler();
        compiler.initServer(server, keyCreator);
        compiler.initDone();
        return compiler;
    }

    public static class JsonResultColumn extends PhysicalResultColumn {
        private DataTypeDescriptor sqlType;
        private TInstance type;
        private PostgresType pgType;
        private List<JsonResultColumn> nestedResultColumns;
        
        public JsonResultColumn(String name, DataTypeDescriptor sqlType, 
                                TInstance type, PostgresType pgType,
                                List<JsonResultColumn> nestedResultColumns) {
            super(name);
            this.sqlType = sqlType;
            this.type = type;
            this.pgType = pgType;
            this.nestedResultColumns = nestedResultColumns;
        }

        public DataTypeDescriptor getSqlType() {
            return sqlType;
        }

        public TInstance getType() {
            return type;
        }

        public PostgresType getPostgresType() {
            return pgType;
        }

        public List<JsonResultColumn> getNestedResultColumns() {
            return nestedResultColumns;
        }
    }

    @Override
    public PhysicalResultColumn getResultColumn(ResultField field) {
        return getJsonResultColumn(field.getName(), field.getSQLtype(), field.getType());
    }

    protected JsonResultColumn getJsonResultColumn(String name, 
                                                   DataTypeDescriptor sqlType, TInstance type) {
        PostgresType pgType = null;
        List<JsonResultColumn> nestedResultColumns = null;
        if (sqlType == null) {
        }
        else if (sqlType.getTypeId().isRowMultiSet()) {
            TypeId.RowMultiSetTypeId typeId = 
                (TypeId.RowMultiSetTypeId)sqlType.getTypeId();
            String[] columnNames = typeId.getColumnNames();
            DataTypeDescriptor[] columnTypes = typeId.getColumnTypes();
            nestedResultColumns = new ArrayList<>(columnNames.length);
            for (int i = 0; i < columnNames.length; i++) {
                nestedResultColumns.add(getJsonResultColumn(columnNames[i], columnTypes[i],
                        getTypesTranslator().typeForSQLType(columnTypes[i])));
            }
        }
        else {
            pgType = PostgresType.fromDerby(sqlType, type);
        }
        return new JsonResultColumn(name, sqlType, type, pgType, nestedResultColumns);
    }

    @Override
    protected PostgresBaseOperatorStatement generateSelect() {
        return new PostgresJsonStatement(this);
    }

    @Override
    protected PostgresBaseOperatorStatement generateSelect(PostgresStatement pstmt,
                                                           PhysicalSelect select,
                                                           PostgresType[] parameterTypes) {
        PostgresJsonStatement pjstmt = (PostgresJsonStatement)pstmt;
        int ncols = select.getResultColumns().size();
        List<JsonResultColumn> resultColumns = new ArrayList<>(ncols);
        for (PhysicalResultColumn physColumn : select.getResultColumns()) {
            JsonResultColumn resultColumn = (JsonResultColumn)physColumn;
            resultColumns.add(resultColumn);
        }
        pjstmt.init(select.getResultOperator(),
                    select.getResultRowType(),
                    resultColumns,
                    parameterTypes,
                    select.getCostEstimate());
        return pjstmt;
    }

    @Override
    protected PostgresBaseOperatorStatement generateUpdate() {
        return super.generateUpdate(); // To handle !returning, see below
    }

    @Override
    protected PostgresBaseOperatorStatement generateUpdate(PostgresStatement pstmt,
                                                           PhysicalUpdate update, String statementType,
                                                           PostgresType[] parameterTypes) {
        if (!update.isReturning()) {
            return super.generateUpdate(pstmt, update, statementType, parameterTypes);
        }
        else {
            int ncols = update.getResultColumns().size();
            List<JsonResultColumn> resultColumns = new ArrayList<>(ncols);
            for (PhysicalResultColumn physColumn : update.getResultColumns()) {
                JsonResultColumn resultColumn = (JsonResultColumn)physColumn;
                resultColumns.add(resultColumn);
            }
            PostgresJsonModifyStatement pjmstmt = new PostgresJsonModifyStatement(this);
            pjmstmt.init(statementType,
                         (Operator)update.getPlannable(),
                         update.getResultRowType(),
                         resultColumns,
                         parameterTypes,
                         update.getCostEstimate(),
                         update.putInCache());
            return pjmstmt;
        }
    }
}
