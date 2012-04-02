/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
        tables = new HashSet<TableSource>();
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
