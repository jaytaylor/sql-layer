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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.ProjectedRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.ProjectedRowType;
import com.foundationdb.qp.rowtype.ProjectedTableRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 <h1>Overview</h1>

 Project_Default computes fields from input rows.  It only operates on rows of one specified type. Rows of other 
 types are passed through without modification.  A common usage is to keep some columns from rows of the specified 
 type and to discard others, but the projection is on expressions, not columns.

 <h1>Arguments</h1>

 <li><b>RowType rowType:</b> Type of rows to be projected. Must be non-null.
 <li><b>List<Expression> projections:</b> Expressions computing fields of output rows. Must be non-null and non-empty.

 <h1>Behavior</h1>

  A row of the specified rowType is projected. Each expression is evaluted using the row as input. The 
 (scalar) outputs from these expressions form a row which is passed to the output stream.

 Rows of other types are passed through from the input stream to the output stream.

 <h1>Output</h1>

  A projected row has a null hkey.

  <h1>Assumptions</h1>

  None.

  <h1>Performance</h1>

  Project_Default does no IO. For each input row, the type is checked and each output field is computed.

  <h1>Memory Requirements</h1>

    None.
 */


class Project_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        if (projectType.hasTable()) {
            return String.format("project to table %s (%s)", projectType.table(), pExpressions.toString());
        } else {
            return String.format("project(%s)", pExpressions.toString());
        }
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, inputOperator.cursor(context, bindingsCursor));
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
        return Collections.singletonList(inputOperator);
    }

    @Override
    public String describePlan()
    {
        return describePlan(inputOperator);
    }

    // Project_Default interface

    public Project_Default(Operator inputOperator, RowType rowType, List<? extends TPreparedExpression> pExpressions)
    {
        ArgumentValidation.notNull("rowType", rowType);
        ArgumentValidation.notEmpty("new projections", pExpressions);

        this.inputOperator = inputOperator;
        this.rowType = rowType;
        this.pExpressions = pExpressions;
        this.projectType = rowType.schema().newProjectType(pExpressions);
    }

    // Project_Default constructor, returns ProjectedTableRowType rows
    public Project_Default(Operator inputOperator, RowType inputRowType,
            RowType projectTableRowType, List<? extends TPreparedExpression> pExpressions)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        ArgumentValidation.notEmpty("new projections", pExpressions);
        
        this.inputOperator = inputOperator;
        this.rowType = inputRowType;
        
        ArgumentValidation.notNull("projectRowType", projectTableRowType);
        ArgumentValidation.isTrue("RowType has Table", projectTableRowType.hasTable());
        projectType = new ProjectedTableRowType(projectTableRowType.schema(),
                                                    projectTableRowType.table(),
                                                    pExpressions);
        this.pExpressions = pExpressions; // TODO defensively copy once the old expressions are gone (until then, this may NPE)
    }


    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Project_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Project_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Project_Default.class);

    // Object state

    protected final Operator inputOperator;
    protected final RowType rowType;
    private final List<? extends TPreparedExpression> pExpressions;
    protected ProjectedRowType projectType;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        if (projectType.hasTable())
            att.put(Label.PROJECT_OPTION, projectType.getExplainer(context));
        att.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        for (TPreparedExpression ex : pExpressions)
            att.put(Label.PROJECTION, ex.getExplainer(context));
        if (context.hasExtraInfo(this))
            att.putAll(context.getExtraInfo(this).get());
        return new CompoundExplainer(Type.PROJECT, att);
    }

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface
        
        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                checkQueryCancelation();
                Row projectedRow = null;
                Row inputRow;
                if ((inputRow = input.next()) != null) {
                    projectedRow =
                        inputRow.rowType() == rowType
                        ? new ProjectedRow(projectType, inputRow, context, bindings, pEvalExpr)
                        : inputRow;
                }
                if (projectedRow == null) {
                    setIdle();
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Project_Default: yield {}", projectedRow);
                }
                return projectedRow;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }


        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            // one list of evaluatables per execution    
            if (pExpressions != null)
                pEvalExpr = ProjectedRow.createTEvaluatableExpressions(pExpressions);
            else
                pEvalExpr = null;
        }

        // Object state
        private List<TEvaluatableExpression> pEvalExpr = null;
    }
}
