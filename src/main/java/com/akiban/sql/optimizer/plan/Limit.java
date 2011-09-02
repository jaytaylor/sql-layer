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

/** LIMIT / OFFSET */
public class Limit extends BasePlanNode
{
    private PlanNode input;
    private int offset, limit;
    private boolean offsetIsParameter, limitIsParameter;

    public Limit(PlanNode input,
                 int offset, boolean offsetIsParameter,
                 int limit, boolean limitIsParameter) {
        this.input = input;
        input.setOutput(this);
        this.offset = offset;
        this.offsetIsParameter = offsetIsParameter;
        this.limit = limit;
        this.limitIsParameter = limitIsParameter;
    }

    public PlanNode getInput() {
        return input;
    }

    public int getOffset() {
        return offset;
    }
    public boolean isOffsetParameter() {
        return offsetIsParameter;
    }
    public int getLimit() {
        return limit;
    }
    public boolean isLimitParameter() {
        return limitIsParameter;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        if (offset > 0) {
            str.append("OFFSET ");
            if (offsetIsParameter) str.append("$");
            str.append(offset);
        }
        if (limit >= 0) {
            str.append("LIMIT ");
            if (limitIsParameter) str.append("$");
            str.append(limit);
        }
        str.append("\n");
        str.append(input);
        return str.toString();
    }

}
