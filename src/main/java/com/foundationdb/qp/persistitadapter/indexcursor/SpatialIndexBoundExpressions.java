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

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.qp.expression.BoundExpressions;
import com.foundationdb.server.types3.pvalue.PValue;
import com.foundationdb.server.types3.pvalue.PValueSource;

class SpatialIndexBoundExpressions implements BoundExpressions
{
    // BoundExpressions interface

    @Override
    public PValueSource pvalue(int position)
    {
        return pValueSources[position];
    }

    // SpatialIndexBoundExpressions interface

    public void value(int position, PValueSource valueSource)
    {
        pValueSources[position] = valueSource;
    }

    public SpatialIndexBoundExpressions(int nFields)
    {
        pValueSources = new PValue[nFields];
    }

    // Object state

    private PValueSource[] pValueSources;
}
