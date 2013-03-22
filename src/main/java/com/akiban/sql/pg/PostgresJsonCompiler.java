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

package com.akiban.sql.pg;

import com.akiban.sql.optimizer.NestedResultSetTypeComputer;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.PhysicalUpdate;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.operator.Operator;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.service.functions.FunctionsRegistry;

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
    protected void initFunctionsRegistry(FunctionsRegistry functionsRegistry) {
        super.initFunctionsRegistry(functionsRegistry);
        typeComputer = new NestedResultSetTypeComputer(functionsRegistry);
    }

    public static PostgresJsonCompiler create(PostgresServerSession server) {
        PostgresJsonCompiler compiler = new PostgresJsonCompiler();
        compiler.initServer(server);
        compiler.initDone();
        return compiler;
    }

    public static class JsonResultColumn extends PhysicalResultColumn {
        private DataTypeDescriptor sqlType;
        private AkType akType;
        private TInstance tInstance;
        private PostgresType pgType;
        private List<JsonResultColumn> nestedResultColumns;
        
        public JsonResultColumn(String name, DataTypeDescriptor sqlType, 
                                AkType akType, TInstance tInstance, PostgresType pgType, 
                                List<JsonResultColumn> nestedResultColumns) {
            super(name);
            this.sqlType = sqlType;
            this.akType = akType;
            this.tInstance = tInstance;
            this.pgType = pgType;
            this.nestedResultColumns = nestedResultColumns;
        }

        public DataTypeDescriptor getSqlType() {
            return sqlType;
        }

        public AkType getAkType() {
            return akType;
        }

        public TInstance getTInstance() {
            return tInstance;
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
        return getJsonResultColumn(field.getName(), field.getSQLtype(), field.getTInstance());
    }

    protected JsonResultColumn getJsonResultColumn(String name, 
                                                   DataTypeDescriptor sqlType, TInstance tInstance) {
        AkType akType;
        PostgresType pgType = null;
        List<JsonResultColumn> nestedResultColumns = null;
        if (sqlType == null)
            akType = AkType.VARCHAR;
        else if (sqlType.getTypeId().isRowMultiSet()) {
            TypeId.RowMultiSetTypeId typeId = 
                (TypeId.RowMultiSetTypeId)sqlType.getTypeId();
            String[] columnNames = typeId.getColumnNames();
            DataTypeDescriptor[] columnTypes = typeId.getColumnTypes();
            nestedResultColumns = new ArrayList<>(columnNames.length);
            for (int i = 0; i < columnNames.length; i++) {
                nestedResultColumns.add(getJsonResultColumn(columnNames[i], columnTypes[i],
                        TypesTranslation.toTInstance(columnTypes[i])));
            }
            akType = AkType.RESULT_SET;
        }
        else {
            akType = TypesTranslation.sqlTypeToAkType(sqlType);
            if (sqlType != null)
                pgType = PostgresType.fromDerby(sqlType, akType, tInstance);
        }
        return new JsonResultColumn(name, sqlType, akType, tInstance, pgType, nestedResultColumns);
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
                    select.getCostEstimate(),
                    usesPValues());
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
                         usesPValues(),
                         update.isRequireStepIsolation(),
                         update.putInCache());
            return pjmstmt;
        }
    }
}
