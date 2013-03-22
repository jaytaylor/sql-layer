
package com.akiban.sql.optimizer.plan;

/** A SQL DELETE statement.
 */
public class DeleteStatement extends BaseUpdateStatement
{
    public DeleteStatement(PlanNode query, TableNode targetTable,
                            TableSource table) {
        super(query, StatementType.DELETE, targetTable, table);
    }
}
