/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

import com.akiban.server.error.InvalidOperationException;

import java.util.Set;

/** A join to an actual table. */
public class TableSource extends BaseJoinable implements ColumnSource
{
    private TableNode table;
    private TableGroup group;
    private TableGroupJoin parentJoin;
    private boolean required;
    private String name;

    public TableSource(TableNode table, boolean required, String name) {
        this.table = table;
        table.addUse(this);
        this.required = required;
        this.name = name;
    }

    public TableNode getTable() {
        return table;
    }

    public TableGroup getGroup() {
        return group;
    }
    public void setGroup(TableGroup group) {
        this.group = group;
        group.getTables().add(this);
    }

    public TableGroupJoin getParentJoin() {
        return parentJoin;
    }
    public void setParentJoin(TableGroupJoin parentJoin) {
        this.parentJoin = parentJoin;
        if (parentJoin != null)
            this.group = parentJoin.getGroup();
    }

    public TableSource getParentTable() {
        if (parentJoin == null)
            return null;
        else
            return parentJoin.getParent();
    }

    public boolean isRequired() {
        return required;
    }
    public boolean isOptional() {
        return !required;
    }
    public void setRequired(boolean required) {
        this.required = required;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isTable() {
        return true;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }
    
    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(name);
        if (parentJoin != null) {
            str.append(" - ");
            str.append(parentJoin);
        }
        else if (group != null) {
            str.append(" - ");
            str.append(group);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        table = map.duplicate(table);
        table.addUse(this);
    }

}
