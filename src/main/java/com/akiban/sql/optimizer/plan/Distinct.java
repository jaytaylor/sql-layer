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

import java.util.List;

/** Make results distinct. */
public class Distinct extends BasePlanWithInput
{
    private List<ExpressionNode> fields;

    public Distinct(PlanNode input, List<ExpressionNode> fields) {
        super(input);
        this.fields = fields;
    }

    public List<ExpressionNode> getFields() {
        return fields;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < fields.size(); i++) {
                        fields.set(i, fields.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (ExpressionNode field : fields) {
                        if (!field.accept((ExpressionVisitor)v))
                            break;
                    }
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        if (fields == null)
            return super.summaryString();
        else
            return super.summaryString() + fields;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

}
