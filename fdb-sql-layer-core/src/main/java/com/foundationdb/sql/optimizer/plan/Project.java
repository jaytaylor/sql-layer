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

import java.util.List;

/** An list of expressions making up new rows. */
public class Project extends BasePlanWithInput implements ColumnSource, TypedPlan
{
    private List<ExpressionNode> fields;

    public Project(PlanNode input, List<ExpressionNode> fields) {
        super(input);
        this.fields = fields;
    }

    public List<ExpressionNode> getFields() {
        return fields;
    }

    @Override
    public String getName() {
        return "PROJECT";
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
    public String summaryString(PlanToString.Configuration configuration) {
        StringBuilder stringBuilder = new StringBuilder(super.summaryString(configuration));
        if (configuration.includeRowTypes) {
            stringBuilder.append('[');
            for (ExpressionNode field : fields) {
                stringBuilder.append(field);
                stringBuilder.append(" (");
                stringBuilder.append(field.getType());
                stringBuilder.append("), ");
            }
            if (fields.size() > 0) {
                stringBuilder.setLength(stringBuilder.length()-2);
            }
            stringBuilder.append(']');
        } else {
            stringBuilder.append(fields);
        }
        return stringBuilder.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

    @Override
    public int nFields() {
        return fields.size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        ExpressionNode field = fields.get(index);
        TPreptimeValue tpv = field.getPreptimeValue();
        return tpv.type();
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        fields.get(index).setPreptimeValue(value);
    }
}
