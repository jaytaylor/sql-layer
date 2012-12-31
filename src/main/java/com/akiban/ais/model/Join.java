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

package com.akiban.ais.model;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.akiban.ais.gwtutils.SerializableEnumSet;

public class Join implements Traversable, HasGroup
{
    public static Join create(AkibanInformationSchema ais,
                              String joinName,
                              UserTable parent,
                              UserTable child)
    {
        ais.checkMutability();
        Join join = new Join(ais, joinName, parent, child);
        join.parent.addCandidateChildJoin(join);
        join.child.addCandidateParentJoin(join);
        ais.addJoin(join);
        return join;
    }

    @Override
    public String toString()
    {
        return
                getGroup() == null
                ? "Join(" + joinName + ": " + child + " -> " + parent + ")"
                : "Join(" + joinName + ": " + child + " -> " + parent + ", group(" + getGroup().getName() + "))";
    }

    public JoinColumn addJoinColumn(Column parent, Column child)
    {
        ais.checkMutability();
        JoinColumn joinColumn = new JoinColumn(this, parent, child);
        joinColumns.add(joinColumn);
        return joinColumn;
    }

    public String getDescription()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(parent);
        buffer.append(" <- ");
        buffer.append(child);
        return buffer.toString();
    }

    public AkibanInformationSchema getAIS()
    {
        return ais;
    }

    public String getName()
    {
        return joinName;
    }

    public UserTable getParent()
    {
        return parent;
    }

    public UserTable getChild()
    {
        return child;
    }

    public Group getGroup()
    {
        return group;
    }

    public void setGroup(Group group)
    {
        this.group = group;
    }

    public Integer getWeight()
    {
        return weight;
    }

    public void setWeight(Integer weight)
    {
        this.weight = weight;
    }

    public List<JoinColumn> getJoinColumns()
    {
        return joinColumns;
    }

    public GroupingUsage getGroupingUsage()
    {
        return groupingUsage;
    }

    public void setGroupingUsage(GroupingUsage usage)
    {
        this.groupingUsage = usage;
    }


    public Set<SourceType> getSourceTypes()
    {
        return sourceTypes;
    }
    
    public int getSourceTypesInt() {
        return sourceTypes.toInt();
    }

    public void setSourceTypes(SerializableEnumSet<SourceType> sourceTypes)
    {
        assert sourceTypes != null;
        this.sourceTypes = sourceTypes;
    }

    public Column getMatchingChild(Column parentColumn)
    {
        for (JoinColumn joinColumn : joinColumns) {
            if (joinColumn.getParent() == parentColumn) {
                return joinColumn.getChild();
            }
        }
        return null;
    }

    public Column getMatchingParent(Column childColumn)
    {
        for (JoinColumn joinColumn : joinColumns) {
            if (joinColumn.getChild() == childColumn) {
                return joinColumn.getParent();
            }
        }
        return null;
    }

    @Override
    public void traversePreOrder(Visitor visitor)
    {
        for (JoinColumn joinColumn : joinColumns) {
            visitor.visitJoinColumn(joinColumn);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor)
    {
        traversePreOrder(visitor);
    }

    public void replaceName(String newName)
    {
        joinName = newName;
    }

    private Join(AkibanInformationSchema ais, String joinName, UserTable parent, UserTable child)
    {
        this.ais = ais;
        this.joinName = joinName;
        this.parent = parent;
        this.child = child;
        joinColumns = new LinkedList<JoinColumn>();
    }

    public enum GroupingUsage
    {
        ALWAYS, NEVER, WHEN_OPTIMAL, IGNORE
    }

    public enum SourceType
    {
        FK, COLUMN_NAME, QUERY, USER
    }

    /**
     * @deprecated - use {@link AkibanInformationSchema#validate(java.util.Collection)}
     * @param out
     * @return
     */
    public boolean checkIntegrity(List<String> out)
    {
        int initialSize = out.size();
        if (joinName == null) {
            out.add("null join name for join: " + this);
        } else if (parent == null) {
            out.add("null parent for join: " + this);
        } else if (child == null) {
            out.add("null child for join: " + this);
        } else if (joinColumns == null) {
            out.add("null join columns for join: " + this);
        } else {
            for (JoinColumn column : joinColumns) {
                if (column == null) {
                    out.add("join contained null column: " + this);
                } else {
                    Column child = column.getChild();
                    Column parent = column.getParent();
                    if (child == null) {
                        out.add("join contained null child column: " + this);
                    } else if (parent == null) {
                        out.add("join contained null parent column: " + this);
                    } else if (!child.getUserTable().equals(this.child)) {
                        out.add("child column's table wasn't child table: " + child + " <--> " + this.child);
                    } else if (!parent.getUserTable().equals(this.parent)) {
                        out.add("parent column's table wasn't parent table: " + child + " <--> " + this.parent);
                    }
                }
            }
        }
        return initialSize == out.size();
    }

    // State

    private final AkibanInformationSchema ais;
    private final UserTable parent;
    private final UserTable child;
    private final List<JoinColumn> joinColumns;
    private String joinName;
    private Integer weight;
    private Group group;

    private GroupingUsage groupingUsage = GroupingUsage.WHEN_OPTIMAL;
    private SerializableEnumSet<SourceType> sourceTypes = new SerializableEnumSet<SourceType>(SourceType.class);
}
