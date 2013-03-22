
package com.akiban.sql.optimizer.plan;

import java.util.List;

import com.akiban.ais.model.Column;

/** A SQL INSERT statement. */
public class InsertStatement extends BaseUpdateStatement
{
    private List<Column> targetColumns;

    public InsertStatement(PlanNode query, TableNode targetTable,
                           List<Column> targetColumns,
                           TableSource table) {
        super(query, StatementType.INSERT, targetTable, table);
        this.targetColumns = targetColumns;
    }

    public List<Column> getTargetColumns() {
        return targetColumns;
    }
    
    @Override
    protected void fillSummaryString(StringBuilder str) {
        super.fillSummaryString(str);
        str.append(targetColumns);
    }
}
