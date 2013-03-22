
package com.akiban.sql.optimizer.plan;

/** Get the table needed for UPDATE / DELETE statements. */
public class UpdateInput extends BasePlanWithInput
{
    private TableSource table;

    public UpdateInput(PlanNode input, 
                       TableSource table) {
        super(input);
        this.table = table;
    }

    public TableSource getTable() {
        return table;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(table.getName());
        str.append(")");
        return str.toString();
    }

}
