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

import com.akiban.ais.model.Group;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** A set of tables with common group joins.
 * These joins need not be contiguous, but group join operators can
 * still be used for access among them.
 */
public class TableGroup extends BasePlanElement
{
    private Group group;
    private Set<TableSource> tables;
    private List<TableGroupJoin> joins, rejectedJoins;

    public TableGroup(Group group) {
        this.group = group;
        tables = new HashSet<TableSource>();
        joins = new ArrayList<TableGroupJoin>();
    }

    public Group getGroup() {
        return group;
    }

    public Set<TableSource> getTables() {
        return tables;
    }

    public List<TableGroupJoin> getJoins() {
        return joins;
    }

    public void addJoin(TableGroupJoin join) {
        joins.add(join);
        tables.add(join.getParent());
        tables.add(join.getChild());
    }

    public List<TableGroupJoin> getRejectedJoins() {
        return rejectedJoins;
    }

    public void rejectJoin(TableGroupJoin join) {
        joins.remove(join);
        if (rejectedJoins == null)
            rejectedJoins = new ArrayList<TableGroupJoin>();
        rejectedJoins.add(join);
    }

    public void merge(TableGroup other) {
        assert (group == other.group);
        for (TableGroupJoin join : other.joins) {
            join.setGroup(this);
            join.getParent().setGroup(this);
            join.getChild().setGroup(this);
            addJoin(join);
        }
    }

    public int getMinOrdinal() {
        int min = Integer.MAX_VALUE;
        for (TableSource table : tables) {
            int ordinal = table.getTable().getOrdinal();
            if (min > ordinal)
                min = ordinal;
        }
        return min;
    }

    public TableSource findByOrdinal(int ordinal) {
        for (TableSource table : tables) {
            if (ordinal == table.getTable().getOrdinal()) {
                return table;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16) +
            // TODO: Fix when Group#toString() gets changed.
            "(" + group.getName().getTableName() + ")";
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tables = duplicateSet(tables, map);
        joins = duplicateList(joins, map);
        if (rejectedJoins != null)
            rejectedJoins = duplicateList(rejectedJoins, map);
    }

}
