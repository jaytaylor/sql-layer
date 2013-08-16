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

import com.foundationdb.ais.model.IndexColumn;

public class FullTextField extends FullTextQuery
{
    public enum Type { MATCH, PARSE, LIKE };

    private ColumnExpression column;
    private Type type;
    private ExpressionNode key;
    private IndexColumn indexColumn;
    
    public FullTextField(ColumnExpression column, Type type, ExpressionNode key) {
        this.column = column;
        this.type = type;
        this.key = key;
    }

    public ColumnExpression getColumn() {
        return column;
    }
    public Type getType() {
        return type;
    }
    public ExpressionNode getKey() {
        return key;
    }

    public IndexColumn getIndexColumn() {
        return indexColumn;
    }
    public void setIndexColumn(IndexColumn indexColumn) {
        this.indexColumn = indexColumn;
    }

    public boolean accept(ExpressionVisitor v) {
        return (column.accept(v) && key.accept(v));
    }

    public void accept(ExpressionRewriteVisitor v) {
        column = (ColumnExpression)column.accept(v);
        key = key.accept(v);
    }

    public FullTextField duplicate(DuplicateMap map) {
        return new FullTextField((ColumnExpression)column.duplicate(map),
                                 type,
                                 (ExpressionNode)key.duplicate(map));
    }
    
    @Override
    public String toString() {
        return type + "(" + column + ", " + key + ")";
    }

}
