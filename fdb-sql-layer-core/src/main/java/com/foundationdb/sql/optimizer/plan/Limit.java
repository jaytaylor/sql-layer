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

/** LIMIT / OFFSET */
public class Limit extends BasePlanWithInput
{
    private int offset, limit;
    private boolean offsetIsParameter, limitIsParameter;

    public Limit(PlanNode input,
                 int offset, boolean offsetIsParameter,
                 int limit, boolean limitIsParameter) {
        super(input);
        this.offset = offset;
        this.offsetIsParameter = offsetIsParameter;
        this.limit = limit;
        this.limitIsParameter = limitIsParameter;
    }

    public Limit(PlanNode input, int limit) {
        this(input, 0, false, limit, false);
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
    public String summaryString(PlanToString.Configuration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(");
        if (offset > 0) {
            str.append("OFFSET ");
            if (offsetIsParameter) str.append("$");
            str.append(offset);
        }
        if (limit >= 0) {
            if (offset > 0) str.append(" ");
            str.append("LIMIT ");
            if (limitIsParameter) str.append("$");
            str.append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
