
package com.akiban.sql.optimizer.plan;

import java.util.List;
import java.util.ArrayList;

public class AncestorLookup extends BaseLookup
{
    private TableNode descendant;
    private List<TableNode> ancestors;

    public AncestorLookup(PlanNode input, TableNode descendant,
                          List<TableNode> ancestors,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant;
        this.ancestors = ancestors;
    }

    public AncestorLookup(PlanNode input, TableSource descendant,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant.getTable();
        this.ancestors = new ArrayList<>(tables.size());
        for (TableSource table : getTables()) {
            ancestors.add(table.getTable());
        }
    }

    public TableNode getDescendant() {
        return descendant;
    }

    public List<TableNode> getAncestors() {
        return ancestors;
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + descendant + " -> " + ancestors + ")";
    }

}
