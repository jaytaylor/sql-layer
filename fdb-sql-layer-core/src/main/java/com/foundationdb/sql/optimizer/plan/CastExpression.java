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

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

/** Cast the result of expression evaluation to a given type.
 */
public class CastExpression extends BaseExpression 
{
    private ExpressionNode inner;

    public CastExpression(ExpressionNode inner, 
                          DataTypeDescriptor sqlType, ValueNode sqlSource,
                          TInstance type) {
        super(sqlType, sqlSource, type);
        this.inner = inner;
    }

    public ExpressionNode getOperand() {
        return inner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CastExpression)) return false;
        CastExpression other = (CastExpression)obj;
        return (getSQLtype().equals(other.getSQLtype()) &&
                inner.equals(other.inner));
    }

    @Override
    public int hashCode() {
        int hash = getSQLtype().hashCode();
        hash += inner.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            inner.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        inner = inner.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        Object typeDescriptor;
        TPreptimeValue tpv = getPreptimeValue();
        if (tpv != null) {
            TInstance type = tpv.type();
            typeDescriptor = type == null ? "<unknown>" : type;
        }
        else {
            typeDescriptor = getSQLtype();
        }
        return "Cast(" + inner + " AS " + typeDescriptor + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        inner = (ExpressionNode)inner.duplicate(map);
    }

}
