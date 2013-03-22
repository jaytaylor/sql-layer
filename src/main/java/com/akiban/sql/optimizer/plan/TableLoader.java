
package com.akiban.sql.optimizer.plan;

import java.util.List;

/** Something that actually introduces data from tables into the stream. */
public interface TableLoader extends PlanElement
{
    public List<TableSource> getTables();
}
