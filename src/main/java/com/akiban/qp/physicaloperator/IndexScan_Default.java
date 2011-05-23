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

package com.akiban.qp.physicaloperator;

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.rowtype.IndexRowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexScan_Default extends PhysicalOperator
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s(%s %s%s)", getClass().getSimpleName(), index, indexKeyRange, reverse ? " reverse" : "");
    }

    // PhysicalOperator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter, indexKeyRange);
    }

    // IndexScan_Default interface

    public IndexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange)
    {
        this.index = indexType.index();
        this.reverse = reverse;
        this.indexKeyRange = indexKeyRange;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexScan_Default.class);

    // Object state

    private final Index index;
    private final boolean reverse;
    private final IndexKeyRange indexKeyRange;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // OperatorExecution interface

        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            cursor.open(bindings);
        }

        @Override
        public boolean next()
        {
            boolean next = cursor.next();
            if (next) {
                outputRow(cursor.currentRow());
            } else {
                close();
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("IndexScan: {}", next ? outputRow() : null);
            }
            return next;
        }

        @Override
        public void close()
        {
            outputRow(null);
            cursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter, IndexKeyRange keyRange)
        {
            this.cursor = adapter.newIndexCursor(index, reverse, keyRange);
        }

        // Object state

        private final Cursor cursor;
    }
}
