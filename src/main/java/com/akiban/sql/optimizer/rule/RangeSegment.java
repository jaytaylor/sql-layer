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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RangeSegment {

    public static List<RangeSegment> fromComparison(Comparison op, ConstantExpression constantExpression) {
        final RangeEndpoint startPoint;
        final RangeEndpoint endPoint;
        Object constantValue = constantExpression.getValue();
        switch (op) {
        case EQ:
            startPoint = endPoint = RangeEndpoint.inclusive(constantValue);
            break;
        case LT:
            startPoint = RangeEndpoint.WILD;
            endPoint = RangeEndpoint.exclusive(constantValue);
            break;
        case LE:
            startPoint = RangeEndpoint.WILD;
            endPoint = RangeEndpoint.inclusive(constantValue);
            break;
        case GT:
            startPoint = RangeEndpoint.exclusive(constantValue);
            endPoint = RangeEndpoint.WILD;
            break;
        case GE:
            startPoint = RangeEndpoint.inclusive(constantValue);
            endPoint = RangeEndpoint.WILD;
            break;
        case NE:
            List<RangeSegment> result = new ArrayList<RangeSegment>(2);
            result.add(fromComparison(Comparison.LT, constantExpression).get(0));
            result.add(fromComparison(Comparison.GT, constantExpression).get(0));
            return result;
        default:
            throw new AssertionError(op.name());
        }
        RangeSegment result = new RangeSegment(startPoint, endPoint);
        return Collections.singletonList(result);
    }

    public RangeEndpoint getStart() {
        return start;
    }

    public RangeEndpoint getEnd() {
        return end;
    }

    @Override
    public String toString() {
        return start + " to " + end;
    }

    public RangeSegment(RangeEndpoint start, RangeEndpoint end) {
        this.start = start;
        this.end = end;
    }

    private RangeEndpoint start;
    private RangeEndpoint end;
}
