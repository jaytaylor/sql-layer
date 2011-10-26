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

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.rowtype.IndexRowType;
import com.persistit.Key;

public class SortCursorDescending extends SortCursorUnidirectional
{
    public static SortCursorDescending create(PersistitAdapter adapter,
                                              RowGenerator rowGenerator,
                                              IndexKeyRange keyRange,
                                              API.Ordering ordering)
    {
        return
            keyRange == null || keyRange.unbounded()
            ? new SortCursorDescending(adapter,
                                       rowGenerator)
            : new SortCursorDescending(adapter,
                                       rowGenerator,
                                       keyRange,
                                       ordering.sortFields());
    }

    private SortCursorDescending(PersistitAdapter adapter, RowGenerator rowGenerator)
    {
        super(adapter, rowGenerator, Key.AFTER, Key.LT);
    }

    private SortCursorDescending(PersistitAdapter adapter,
                                 RowGenerator rowGenerator,
                                 IndexKeyRange keyRange,
                                 int sortFields)
    {
        super(adapter,
              rowGenerator,
              keyRange.indexRowType(),
              sortFields,
              keyRange.hi(),
              keyRange.hiInclusive(),
              keyRange.lo(),
              keyRange.loInclusive(),
              -1);
    }
}
