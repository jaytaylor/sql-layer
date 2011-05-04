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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexScan_Default extends PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public OperatorExecution cursor(StoreAdapter adapter, Bindings bindings)
    {
        return new Execution(adapter, indexKeyRangeBindable.bindTo(bindings));
    }

    @Override
    public String toString() 
    {
        return String.format("%s(%s)", getClass().getSimpleName(), index);
    }

    // IndexScan_Default interface

    public IndexScan_Default(Index index, boolean reverse, Bindable<IndexKeyRange> indexKeyRangeBindable)
    {
        this.index = index;
        this.reverse = reverse;
        this.indexKeyRangeBindable = indexKeyRangeBindable;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexScan_Default.class);

    // Object state

    private final Index index;
    private final boolean reverse;
    private final Bindable<IndexKeyRange> indexKeyRangeBindable;

    // Inner classes

    private class Execution extends SingleRowCachingCursor
    {
        // OperatorExecution interface

        // Cursor interface

        @Override
        public void open()
        {
            cursor.open();
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
            super(adapter);
            this.cursor = adapter.newIndexCursor(index, reverse, keyRange);
        }

        // Object state

        private final Cursor cursor;
    }
}
