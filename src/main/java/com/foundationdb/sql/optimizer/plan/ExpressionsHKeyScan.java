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

import com.foundationdb.ais.model.HKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ExpressionsHKeyScan extends BaseScan implements EqualityColumnsScan
{
    private TableSource table;
    private HKey hKey;
    private List<ExpressionNode> columns;
    private List<ExpressionNode> keys;
    private List<ConditionExpression> conditions;

    public ExpressionsHKeyScan(TableSource table) {
        this.table = table;
        this.hKey = table.getTable().getTable().hKey();
    }

    public TableSource getTable() {
        return table;
    }

    public HKey getHKey() {
        return hKey;
    }

    @Override
    public TableSource getLeafMostTable() {
        return table;
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return columns;
    }

    public void setColumns(List<ExpressionNode> columns) {
        this.columns = columns;
    }

    public List<ExpressionNode> getKeys() {
        return keys;
    }

    @Override
    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    @Override
    public void addEqualityCondition(ConditionExpression condition, 
                                     ExpressionNode comparand) {
        if (conditions == null) {
            int ncols = hKey.nColumns();
            conditions = new ArrayList<>(ncols);
            keys = new ArrayList<>(ncols);            
        }
        conditions.add(condition);
        keys.add(comparand);
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                visitComparands((ExpressionRewriteVisitor)v);
            }
            else if (v instanceof ExpressionVisitor) {
                visitComparands((ExpressionVisitor)v);
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
        for (int i = 0; i < keys.size(); i++) {
            keys.set(i, keys.get(i).accept(v));
        }
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
        for (ExpressionNode key : keys) {
            key.accept(v);
        }
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append('(');
        str.append(table);
        for (ExpressionNode key : keys) {
            str.append(", ");
            str.append(key);
        }
        if (getCostEstimate() != null) {
            str.append(", ");
            str.append(getCostEstimate());
        }
        str.append(")");
        return str.toString();
    }

}
