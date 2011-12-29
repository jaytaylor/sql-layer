/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.optimizer.plan.PhysicalSelect;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.plan.TypesTranslation;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.types.AkType;

import java.util.*;

public class PostgresJsonCompiler extends PostgresOperatorCompiler
{
    public PostgresJsonCompiler(PostgresServerSession server) {
        super(server);
    }    

    public static class JsonResultColumn extends PhysicalResultColumn {
        private AkType akType;
        
        public JsonResultColumn(String name, AkType akType) {
            super(name);
            this.akType = akType;
        }

        public AkType getAkType() {
            return akType;
        }
    }

    @Override
    public PhysicalResultColumn getResultColumn(ResultField field) {
        DataTypeDescriptor sqlType = field.getSQLtype();
        AkType akType;
        if (sqlType == null)
            akType = AkType.VARCHAR;
        else
            akType = TypesTranslation.sqlTypeToAkType(sqlType);
        // TODO: Nested.
        return new JsonResultColumn(field.getName(), akType);
    }

    @Override
    protected PostgresStatement generateSelect(PhysicalSelect select,
                                               PostgresType[] parameterTypes, List<EnvironmentExpressionSetting> environmentSettings) {
        int ncols = select.getResultColumns().size();
        List<JsonResultColumn> resultColumns = new ArrayList<JsonResultColumn>();
        for (PhysicalResultColumn physColumn : select.getResultColumns()) {
            JsonResultColumn resultColumn = (JsonResultColumn)physColumn;
            resultColumns.add(resultColumn);
        }
        return new PostgresJsonStatement(select.getResultOperator(),
                                         select.getResultRowType(),
                                         resultColumns,
                                         parameterTypes, environmentSettings);
    }
    
}
