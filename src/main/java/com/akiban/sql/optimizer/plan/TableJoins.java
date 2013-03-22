
package com.akiban.sql.optimizer.plan;

import java.util.HashSet;
import java.util.Set;

/** A contiguous set of tables joined together: flattened / producted
 * and acting as a single row set for higher level joins.
 */
public class TableJoins extends BasePlanWithInput implements Joinable
{
    private TableGroup group;
    private Set<TableSource> tables;
    private PlanNode scan;

    public TableJoins(Joinable joins, TableGroup group) {
        super(joins);
        this.group = group;
        tables = new HashSet<>();
    }

    public TableGroup getGroup() {
        return group;
    }

    public Joinable getJoins() {
        return (Joinable)getInput();
    }

    public Set<TableSource> getTables() {
        return tables;
    }

    public void addTable(TableSource table) {
        assert (group == table.getGroup());
        tables.add(table);
    }

    public PlanNode getScan() {
        return scan;
    }
    public void setScan(PlanNode scan) {
        this.scan = scan;
    }

    @Override
    public boolean isTable() {
        return false;
    }
    @Override
    public boolean isGroup() {
        return true;
    }
    @Override
    public boolean isJoin() {
        return false;
    }
    @Override
    public boolean isInnerJoin() {
        return false;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(group);
        if (scan != null) {
            str.append(" - ");
            str.append(scan.summaryString());
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        group = (TableGroup)group.duplicate(map);
        tables = duplicateSet(tables, map);
    }

}
