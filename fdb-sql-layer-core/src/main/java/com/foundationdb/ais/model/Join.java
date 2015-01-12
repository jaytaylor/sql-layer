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

import com.foundationdb.ais.model.validation.AISInvariants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Join implements HasGroup, Constraint
{
    public static Join create(AkibanInformationSchema ais,
                              String joinName,
                              Table parent,
                              Table child)
    {
        ais.checkMutability();
        Join join = new Join(joinName, parent, child);
        join.parent.addCandidateChildJoin(join);
        join.child.addCandidateParentJoin(join);
        AISInvariants.checkDuplicateConstraintsInSchema(ais, join.getConstraintName());
        ais.addJoin(join);
        ais.addConstraint(join);
        return join;
    }

    // used by the Foreign Key to track internal joins. 
    protected static Join create (String joinName, Table parent, Table child) {
        Join join = new Join (joinName, parent, child);
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
        assert this.childColumns == null : "Modifying fixed Join child columns";
        assert this.parentColumns == null: "Modifying fixed Join parent columns";
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

    public List<Column> getChildColumns() {
        if (this.childColumns == null) {
            List<Column> childColumns = new ArrayList<Column>(joinColumns.size());
            for (JoinColumn joinColumn : joinColumns) {
                childColumns.add(joinColumn.getChild());
            }
            this.childColumns =  Collections.unmodifiableList(childColumns);
        }
        return this.childColumns;
    }
    
    public List<Column> getParentColumns()  {
        if (this.parentColumns == null) {
            List<Column> parentColumns = new ArrayList<Column>(joinColumns.size());
            for (JoinColumn joinColumn : joinColumns) {
               parentColumns.add(joinColumn.getParent());
            }
            this.parentColumns = Collections.unmodifiableList(parentColumns);
        }
        return this.parentColumns;
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

    
    public void replaceName(String newName)
    {
        joinName = newName;
    }

    @Override
    public Table getConstraintTable() {
        return child;
    }

    @Override
    public TableName getConstraintName(){
        return constraintName;
    }
    
    private Join (String joinName, Table parent, Table child) {
        this.joinName = joinName;
        this.parent = parent;
        this.child = child;
        joinColumns = new LinkedList<>();
        this.constraintName = new TableName(parent.getName().getSchemaName(), joinName);
    }
    // State

    private final Table parent;
    private final Table child;
    private final List<JoinColumn> joinColumns;
    private List<Column> childColumns;
    private List<Column> parentColumns;
    private String joinName;
    private Group group;
    private TableName constraintName;
}
