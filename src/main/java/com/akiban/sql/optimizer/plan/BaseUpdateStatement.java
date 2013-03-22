
package com.akiban.sql.optimizer.plan;

/** A statement that modifies the database.
 */
public class BaseUpdateStatement extends BasePlanWithInput
{
    public enum StatementType {
        DELETE,
        INSERT,
        UPDATE
    }
    
    private TableNode targetTable;
    private TableSource table;
    private final StatementType type;
    
    protected BaseUpdateStatement(PlanNode query, StatementType type, TableNode targetTable,
                                    TableSource table) {
        super(query);
        this.type = type;
        this.targetTable = targetTable;
        this.table = table;
    }

    public TableNode getTargetTable() {
        return targetTable;
    }


    public TableSource getTable() { 
        return table;
    }

    public StatementType getType() {
        return type;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append('(');
        fillSummaryString(str);
        //if (requireStepIsolation)
        //    str.append(", HALLOWEEN");
        str.append(')');
        return str.toString();
    }

    protected void fillSummaryString(StringBuilder str) {
        str.append(getTargetTable());
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        targetTable = map.duplicate(targetTable);
    }
}
