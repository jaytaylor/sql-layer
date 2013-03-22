
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.UserTable;

import java.util.List;
import java.util.ArrayList;

/** A table from AIS.
 */
public class TableNode extends TableTreeBase.TableNodeBase<TableNode> 
{
    private TableTree tree;
    private List<TableSource> uses;
    private long branches;

    public TableNode(UserTable table, TableTree tree) {
        super(table);
        this.tree = tree;
        uses = new ArrayList<>();
    }

    public TableTree getTree() {
        return tree;
    }

    public void addUse(TableSource use) {
        uses.add(use);
    }

    public long getBranches() {
        return branches;
    }
    public void setBranches(long branches) {
        this.branches = branches;
    }

}
