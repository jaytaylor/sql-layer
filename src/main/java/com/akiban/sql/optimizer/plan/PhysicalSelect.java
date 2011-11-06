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

package com.akiban.sql.optimizer.plan;

import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.types.DataTypeDescriptor;

import static com.akiban.server.expression.std.EnvironmentExpression.EnvironmentValue;

import java.util.List;
import java.util.Arrays;

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

    private List<PhysicalResultColumn> resultColumns;
    private RowType rowType;
    
    public PhysicalSelect(Operator resultOperator, RowType rowType,
                          List<PhysicalResultColumn> resultColumns,
                          DataTypeDescriptor[] parameterTypes,
                          List<EnvironmentValue> environmentValues) {
        super(resultOperator, parameterTypes, environmentValues);
        this.rowType = rowType;
        this.resultColumns = resultColumns;
    }

    public Operator getResultOperator() {
        return (Operator)getPlannable();
    }

    public RowType getResultRowType() {
        return rowType;
    }

    public List<PhysicalResultColumn> getResultColumns() {
        return resultColumns;
    }

    @Override
    public boolean isUpdate() {
        return false;
    }
    
    @Override
    protected String withIndentedExplain(StringBuilder str) {
        if (getParameterTypes() != null)
            str.append(Arrays.toString(getParameterTypes()));
        if (getEnvironmentValues() != null)
            str.append(getEnvironmentValues());
        str.append(resultColumns);
        return super.withIndentedExplain(str);
    }

}
