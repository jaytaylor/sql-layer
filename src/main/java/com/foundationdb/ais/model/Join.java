/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model;

import java.util.LinkedList;
import java.util.List;

public class Join implements Traversable, HasGroup
{
    public static Join create(AkibanInformationSchema ais,
                              String joinName,
                              Table parent,
                              Table child)
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

    public Table getParent()
    {
        return parent;
    }

    public Table getChild()
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

    public List<JoinColumn> getJoinColumns()
    {
        return joinColumns;
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

    private Join(AkibanInformationSchema ais, String joinName, Table parent, Table child)
    {
        this.ais = ais;
        this.joinName = joinName;
        this.parent = parent;
        this.child = child;
        joinColumns = new LinkedList<>();
    }

    // State

    private final AkibanInformationSchema ais;
    private final Table parent;
    private final Table child;
    private final List<JoinColumn> joinColumns;
    private String joinName;
    private Group group;
}
