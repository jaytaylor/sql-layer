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

import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import com.akiban.ais.model.Column;

import java.util.Set;

/** An expression evaluating a column in an actual table. */
public class ColumnExpression extends BaseExpression 
{
    private ColumnSource table;
    private Column column;
    private int position;
    private EquivalenceFinder<ColumnExpression> equivalenceFinder;

    public ColumnExpression(TableSource table, Column column, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource,
                            EquivalenceFinder<ColumnExpression> equivalenceFinder) {
        super(sqlType, column.getType().akType(), sqlSource);
        this.table = table;
        assert (table.getTable().getTable() == column.getUserTable());
        this.column = column;
        this.position = column.getPosition();
        this.equivalenceFinder = equivalenceFinder;
    }

    public ColumnExpression(ColumnSource table, int position,
                            DataTypeDescriptor sqlType, AkType type, ValueNode sqlSource) {
        super(sqlType, type, sqlSource);
        this.table = table;
        this.position = position;
    }

    public ColumnExpression(ColumnSource table, int position, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.table = table;
        this.position = position;
    }

    @Override
    public void setSQLtype(DataTypeDescriptor type) {
        super.setSQLtype(type);
    }

    // Generated column references without an original SQL source.
    public ColumnExpression(TableSource table, Column column) {
        this(table, column, null, null, null);
    }

    public ColumnSource getTable() {
        return table;
    }

    public Column getColumn() {
        return column;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public void markEquivalentTo(ColumnExpression other) {
        equivalenceFinder.markEquivalent(this, other);
    }

    public EquivalenceFinder<ColumnExpression> getEquivalenceFinder() {
        if (equivalenceFinder == null)
            throw new IllegalStateException("no equivalence finder");
        return equivalenceFinder;
    }
    
    public Set<ColumnExpression> getEquivalents() {
        return getEquivalenceFinder().findEquivalents(this);
    }

    public Set<ColumnExpression> getEquivalentsPlusSelf() {
        Set<ColumnExpression> equivalents = getEquivalents();
        equivalents.add(this);
        return equivalents;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnExpression)) return false;
        ColumnExpression other = (ColumnExpression)obj;
        return (table.equals(other.table) &&
                ((column != null) ?
                 (column == other.column) :
                 (position == other.position)));
    }

    @Override
    public int hashCode() {
        int hash = table.hashCode();
        hash += position;
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        return v.visit(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        if (column != null)
            return table.getName() + "." + column.getName();
        else
            return table.getName() + "[" + position + "]";
    }

    @Override
    public boolean isColumn() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        table = (ColumnSource)table.duplicate(map);
    }
}
