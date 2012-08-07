/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.qp.operator;

import com.akiban.qp.row.ProjectedRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.ProjectedRowType;
import com.akiban.qp.rowtype.ProjectedUserTableRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedExpressions;
import com.akiban.sql.optimizer.explain.*;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.tap.InOutTap;

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
        Format f = new Format(true);
        StringBuilder sb = new StringBuilder();
        for (String row : f.Describe(this.getExplainer(null)))
        {
            sb.append(row).append('\n');
        }
        sb.setLength(sb.length()-1);
        return sb.toString();
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

    public Project_Default(Operator inputOperator, RowType rowType, List<? extends Expression> projections, List<? extends TPreparedExpression> pExpressions)
    {
        ArgumentValidation.notNull("rowType", rowType);
        if (projections == null && pExpressions == null)
            throw new IllegalArgumentException("either projections or pExpressions must be present");
        if (projections == null)
            ArgumentValidation.isGT("pExpressions.size()", pExpressions.size(), 0);
        else if (pExpressions == null)
            ArgumentValidation.isGT("projections.size()", projections.size(), 0);
        else
            throw new IllegalArgumentException("only one of projections or pExpressions must be present");
        this.inputOperator = inputOperator;
        this.rowType = rowType;
        this.pExpressions = pExpressions;
        this.projections = projections;
        projectType = rowType.schema().newProjectType(this.projections, pExpressions);
    }

    // Project_Default constructor, returns ProjectedUserTableRowType rows 
    public Project_Default(Operator inputOperator, RowType inputRowType,
            RowType projectTableRowType, List<? extends Expression> projections, List<? extends TPreparedExpression> pExpressions)
    {
        ArgumentValidation.notNull("inputRowType", inputRowType);
        if (pExpressions != null)
            ArgumentValidation.notEmpty("new projections", pExpressions);
        else if (projections != null)
            ArgumentValidation.notEmpty("new projections", projections);
        else
            throw new IllegalArgumentException("both expressions lists can't be null");
        assert (projections == null) || (pExpressions == null) : "both expressions lists can't be non-null";
        
        this.inputOperator = inputOperator;
        this.rowType = inputRowType;
        this.projections = projections;
        
        ArgumentValidation.notNull("projectRowType", projectTableRowType);
        ArgumentValidation.isTrue("RowType has UserTable", projectTableRowType.hasUserTable());
        projectType = new ProjectedUserTableRowType(projectTableRowType.schema(),
                                                    projectTableRowType.userTable(),
                                                    projections,
                                                    pExpressions);
        this.pExpressions = pExpressions; // TODO defensively copy once the old expressions are gone (until then, this may NPE)
    }


    // Class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Project_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Project_Default next");
    
    // Object state

    protected final Operator inputOperator;
    protected final RowType rowType;
    protected final List<? extends Expression> projections;
    private final List<? extends TPreparedExpression> pExpressions;
    protected ProjectedRowType projectType;

    @Override
    public Explainer getExplainer(Map<Object, Explainer> extraInfo)
    {
        Attributes att = new Attributes();
        
        att.put(Label.NAME, PrimitiveExplainer.getInstance("project"));
        if (projectType.hasUserTable())
            att.put(Label.PROJECT_OPTION, PrimitiveExplainer.getInstance("Has User Table: " + projectType.userTable()));
        att.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(extraInfo));
        if (projections != null) {
            for (Expression ex : projections)
                att.put(Label.PROJECTION, ex.getExplainer(extraInfo));
        }
        else {
            for (TPreparedExpression ex : pExpressions)
                att.put(Label.PROJECTION, TPreparedExpressions.getExplainer(ex));
        }
        return new OperationExplainer(Type.PROJECT, att);
    }

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // Cursor interface

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                input.open();
                idle = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            TAP_NEXT.in();
            try {
                CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                Row projectedRow = null;
                Row inputRow;
                if ((inputRow = input.next()) != null) {
                    projectedRow =
                        inputRow.rowType() == rowType
                        ? new ProjectedRow(projectType, inputRow, context, projections, pExpressions)
                        : inputRow;
                }
                if (projectedRow == null) {
                    close();
                }
                return projectedRow;
            } finally {
                TAP_NEXT.out();
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!idle) {
                input.close();
                idle = true;
            }
        }

        @Override
        public void destroy()
        {
            if (input != null) {
                close();
                input.destroy();
                input = null;
            }
        }

        @Override
        public boolean isIdle()
        {
            return input != null && idle;
        }

        @Override
        public boolean isActive()
        {
            return input != null && !idle;
        }

        @Override
        public boolean isDestroyed()
        {
            return input == null;
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context);
            this.input = input;
        }

        // Object state

        private Cursor input; // input = null indicates destroyed.
        private boolean idle = true;
    }
}
