
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.IndexColumn;

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
