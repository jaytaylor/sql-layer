
package com.akiban.sql.optimizer.plan;

import java.util.List;

public class BranchLookup extends BaseLookup
{
    private TableNode source, ancestor, branch;

    public BranchLookup(PlanNode input, 
                        TableNode source, TableNode ancestor, TableNode branch,
                        List<TableSource> tables) {
        super(input, tables);
        this.source = source;
        this.ancestor = ancestor;
        this.branch = branch;
    }

    /** Lookup a branch right right beneath a starting point. */
    public BranchLookup(PlanNode input, TableNode source, List<TableSource> tables) {
        this(input, source, source, source, tables);
    }

    /** Lookup an immediate child of the starting point. */
    public BranchLookup(PlanNode input, TableNode source, TableNode branch, 
                        List<TableSource> tables) {
        this(input, source, source, branch, tables);
        assert(source == branch.getParent());
    }

    public TableNode getSource() {
        return source;
    }

    public TableNode getAncestor() {
        return ancestor;
    }

    public TableNode getBranch() {
        return branch;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(").append(source).append(" -> ").append(branch);
        if (ancestor != source)
            str.append(" via ").append(ancestor);
        str.append(")");
        return str.toString();
    }

}
