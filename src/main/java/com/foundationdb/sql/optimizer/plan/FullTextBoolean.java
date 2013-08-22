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

import static com.foundationdb.server.service.text.FullTextQueryBuilder.BooleanType;

import java.util.ArrayList;
import java.util.List;

public class FullTextBoolean extends FullTextQuery
{
    private List<FullTextQuery> operands;
    private List<BooleanType> types;

    public FullTextBoolean(List<FullTextQuery> operands, List<BooleanType> types) {
        this.operands = operands;
        this.types = types;
    }

    public List<FullTextQuery> getOperands() {
        return operands;
    }
    public List<BooleanType> getTypes() {
        return types;
    }

    public boolean accept(ExpressionVisitor v) {
        for (FullTextQuery operand : operands) {
            if (!operand.accept(v)) {
                return false;
            }
        }
        return true;
    }

    public void accept(ExpressionRewriteVisitor v) {
        for (FullTextQuery operand : operands) {
            operand.accept(v);
        }
    }

    public FullTextBoolean duplicate(DuplicateMap map) {
        List<FullTextQuery> newOperands = new ArrayList<>(operands.size());
        for (FullTextQuery operand : operands) {
            newOperands.add((FullTextQuery)operand.duplicate(map));
        }
        return new FullTextBoolean(newOperands, new ArrayList<>(types));
    }
    
    
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("[");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                str.append(", ");
            }
            str.append(types.get(i));
            str.append("(");
            str.append(operands.get(i));
            str.append(")");
        }
        str.append("]");
        return str.toString();
    }

}
