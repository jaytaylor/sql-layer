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

import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.physicaloperator.PhysicalOperator;

import java.util.List;

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
    
    public PhysicalSelect(PhysicalOperator resultOperator,
                          List<PhysicalResultColumn> resultColumns,
                          DataTypeDescriptor[] parameterTypes) {
        super(resultOperator, parameterTypes);
        this.resultColumns = resultColumns;
    }

    public PhysicalOperator getResultOperator() {
        return (PhysicalOperator)getPlannable();
    }

    public List<PhysicalResultColumn> getResultColumns() {
        return resultColumns;
    }

    @Override
    public boolean isUpdate() {
        return false;
    }
    
    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        if (parameterTypes != null)
            str.append(parameterTypes);
        str.append(resultColumns);
        for (String operator : explainPlan()) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}
