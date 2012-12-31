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

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import com.akiban.ais.model.Column;

/** An expression evaluating a column in an actual table. */
public class ColumnExpression extends BaseExpression 
{
    private ColumnSource table;
    private Column column;
    private int position;

    public ColumnExpression(TableSource table, Column column, 
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, column.getType().akType(), sqlSource);
        this.table = table;
        assert (table.getTable().getTable() == column.getUserTable());
        this.column = column;
        this.position = column.getPosition();
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
