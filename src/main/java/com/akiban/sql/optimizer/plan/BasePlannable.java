
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.UserTable;
import com.akiban.sql.optimizer.plan.PhysicalSelect.PhysicalResultColumn;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.exec.Plannable;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.explain.ExplainContext;
import com.akiban.server.explain.format.DefaultFormatter;

import java.util.*;

/** Physical operator plan */
public abstract class BasePlannable extends BasePlanNode
{
    private Plannable plannable;
    private DataTypeDescriptor[] parameterTypes;
    private List<PhysicalResultColumn> resultColumns;
    private RowType rowType;
    private CostEstimate costEstimate;
    private Set<UserTable> affectedTables;

    protected BasePlannable(Plannable plannable,
                            DataTypeDescriptor[] parameterTypes,
                            RowType rowType,
                            List<PhysicalResultColumn> resultColumns,
                            CostEstimate costEstimate,
                            Set<UserTable> affectedTables) {
        this.plannable = plannable;
        this.parameterTypes = parameterTypes;
        this.rowType = rowType;
        this.resultColumns = resultColumns;
        this.costEstimate = costEstimate;
        this.affectedTables = affectedTables;
    }

    public Plannable getPlannable() {
        return plannable;
    }
    public DataTypeDescriptor[] getParameterTypes() {
        return parameterTypes;
    }

    public RowType getResultRowType() {
        return rowType;
    }

    public List<PhysicalResultColumn> getResultColumns() {
        return resultColumns;
    }

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }

    public Set<UserTable> getAffectedTables() {
        return affectedTables;
    }

    public abstract boolean isUpdate();

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy operators.
    }
    
    public String explainToString(ExplainContext context, String defaultSchemaName) {
        return withIndentedExplain(new StringBuilder(getClass().getSimpleName()), context, defaultSchemaName);
    }

    @Override
    public String toString() {
        return explainToString(null, null);
    }

    @Override
    public String summaryString() {
        // Similar to above, but with @hash for consistency.
        return withIndentedExplain(new StringBuilder(super.summaryString()), null, null);
    }

    protected String withIndentedExplain(StringBuilder str, ExplainContext context, String defaultSchemaName) {
        if (context == null)
            context = new ExplainContext(); // Empty
        DefaultFormatter f = new DefaultFormatter(defaultSchemaName);
        for (String operator : f.format(plannable.getExplainer(context))) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}
