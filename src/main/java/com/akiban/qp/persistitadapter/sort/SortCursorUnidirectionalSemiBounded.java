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

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.expression.BoundExpressions;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.types.conversion.Converters;

// For a semi-bounded (mysqlish) index scan

class SortCursorUnidirectionalSemiBounded extends SortCursorUnidirectional
{
    // SortCursorUnidirectional interface

    public static SortCursorUnidirectionalSemiBounded create(PersistitAdapter adapter,
                                                             IterationHelper iterationHelper,
                                                             IndexKeyRange keyRange,
                                                             API.Ordering ordering)
    {
        return new SortCursorUnidirectionalSemiBounded(adapter, iterationHelper, keyRange, ordering);
    }

    // For use by this class

    private SortCursorUnidirectionalSemiBounded(PersistitAdapter adapter,
                                                IterationHelper iterationHelper,
                                                IndexKeyRange keyRange,
                                                API.Ordering ordering)
    {
        super(adapter, iterationHelper, keyRange, ordering);
    }

    protected void evaluateBoundaries(Bindings bindings)
    {
        if (start == null) {
            startKey = null;
        } else {
            BoundExpressions startExpressions = start.boundExpressions(bindings, adapter);
            startKey.clear();
            startKeyTarget.attach(startKey);
            for (int f = 0; f < boundColumns; f++) {
                startKeyTarget.expectingType(types[f]);
                Converters.convert(startExpressions.eval(f), startKeyTarget);
            }
        }
        if (end == null) {
            endKey = null;
        } else {
            BoundExpressions endExpressions = end.boundExpressions(bindings, adapter);
            endKey.clear();
            endKeyTarget.attach(endKey);
            for (int f = 0; f < boundColumns; f++) {
                endKeyTarget.expectingType(types[f]);
                Converters.convert(endExpressions.eval(f), endKeyTarget);
            }
        }
    }
}
