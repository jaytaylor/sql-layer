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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.sql.types.DataTypeDescriptor;

import java.util.List;
import java.util.Arrays;
import java.util.Set;

/** Physical SELECT query */
public class PhysicalSelect extends BasePlannable
{
    // Probably subclassed by specific client to capture typing information in some way.
    public static class PhysicalResultColumn {
        private String name;
        
        public PhysicalResultColumn(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public PhysicalSelect(Operator resultOperator, RowType rowType,
                          List<PhysicalResultColumn> resultColumns,
                          ParameterType[] parameterTypes,
                          CostEstimate costEstimate,
                          Set<Table> affectedTables) {
        super(resultOperator, parameterTypes, rowType, resultColumns, costEstimate, affectedTables);
    }

    public Operator getResultOperator() {
        return (Operator)getPlannable();
    }


    @Override
    public boolean isUpdate() {
        return false;
    }
    
    @Override
    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName, DefaultFormatter.LevelOfDetail levelOfDetail) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        str.append(getResultColumns());
        return super.withIndentedExplain(str, context, defaultSchemaName, levelOfDetail);
    }

}
