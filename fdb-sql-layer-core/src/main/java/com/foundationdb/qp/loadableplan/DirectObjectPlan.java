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

package com.foundationdb.qp.loadableplan;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;

/** A plan that uses a {@link DirectObjectCursor}. */
public abstract class DirectObjectPlan
{
    public abstract DirectObjectCursor cursor(QueryContext context, QueryBindings bindings);

    public enum TransactionMode { NONE, READ_ONLY, READ_WRITE };

    public enum OutputMode { TABLE, COPY_WITH_NEWLINE, COPY };

    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
    }

    /** Return <code>COPY</code> to stream a single column with text formatting. */
    public OutputMode getOutputMode() {
        return OutputMode.TABLE;
    }
}
