
package com.akiban.sql.optimizer.rule.cost;

import com.akiban.ais.model.Table;

public interface TableRowCounts
{
    public long getTableRowCount(Table table);
}
