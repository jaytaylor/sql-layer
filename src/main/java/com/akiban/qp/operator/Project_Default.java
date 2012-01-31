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

package com.akiban.qp.operator;

import com.akiban.qp.row.ProjectedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.ProjectedUserTableRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class Project_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        if (projectType.hasUserTable()) {
            return String.format("project to table %s (%s)", projectType.userTable(), projections);
        } else {
            return String.format("project(%s)", projections);
        }
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context)
    {
        return new Execution(context, inputOperator.cursor(context));
    }

    @Override
    public ProjectedRowType rowType()
    {
        return projectType;
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(projectType);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<Operator>(1);
        result.add(inputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Project_Default interface

    public Project_Default(Operator inputOperator, RowType rowType, List<? extends Expression> projections)
    {
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notEmpty("projections", projections);
        this.inputOperator = inputOperator;
        this.rowType = rowType;
        this.projections = new ArrayList<Expression>(projections);
        projectType = rowType.schema().newProjectType(this.projections);
    }
    
    // Project_Default constructor, returns ProjectedUserTableRowType rows 
    public Project_Default(Operator inputOperator, RowType inputRowType,
            RowType projectTableRowType, List<? extends Expression> projections)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notEmpty("projections", projections);
        
        this.inputOperator = inputOperator;
        this.rowType = inputRowType;
        this.projections = new ArrayList<Expression>(projections);
        
        ArgumentValidation.notNull("projectRowType", projectTableRowType);
        ArgumentValidation.isTrue("RowType has UserTable", projectTableRowType.hasUserTable());
        projectType = new ProjectedUserTableRowType(projectTableRowType.schema(),
                                                    projectTableRowType.userTable(),
                                                    projections);
    }


    // Object state

    protected final Operator inputOperator;
    protected final RowType rowType;
    protected final List<Expression> projections;
    protected ProjectedRowType projectType;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            input.open();
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row projectedRow = null;
            Row inputRow;
            if ((inputRow = input.next()) != null) {
                projectedRow =
                    inputRow.rowType() == rowType
                    ? new ProjectedRow(projectType, inputRow, context, projections)
                    : inputRow;
            }
            return projectedRow;
        }

        @Override
        public void close()
        {
            input.close();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }

        // Object state

        private final Cursor input;
    }
}
