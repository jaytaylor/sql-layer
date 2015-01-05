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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.types.TInstance;

/** An expression evaluating a column in an actual table. */
public class ColumnExpression extends BaseExpression 
{
    private ColumnSource table;
    private Column column;
    private int position;

    public ColumnExpression(TableSource table, Column column, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource, column.getType());
        this.table = table;
        assert (table.getTable().getTable() == column.getTable());
        this.column = column;
        this.position = column.getPosition();
    }

    public ColumnExpression(ColumnExpression c, ColumnSource columnSource){
        super(c.getSQLtype(), c.getSQLsource(), c.getType());
        this.table = columnSource;
        this.position = c.getPosition();
    }
    
    public ColumnExpression(ColumnSource table, int position, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource,
                            TInstance type) {
        super(sqlType, sqlSource, type);
        this.table = table;
        this.position = position;
    }

    // Generated column references without an original SQL source.
    public ColumnExpression(TableSource table, Column column) {
        this(table, column, null, null);
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
