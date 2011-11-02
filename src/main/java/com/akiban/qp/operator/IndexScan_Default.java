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

package com.akiban.qp.operator;

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.util.ArgumentValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IndexScan_Default extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        str.append("(").append(index);
        str.append(" ").append(indexKeyRange);
        if (reverse)
            str.append(" reverse");
        str.append(scanSelector.describe());
        str.append(")");
        return str.toString();
    }

    // Operator interface

    @Override
    protected Cursor cursor(StoreAdapter adapter)
    {
        return new Execution(adapter);
    }

    // IndexScan_Default interface

    public IndexScan_Default(IndexRowType indexType,
                             boolean reverse,
                             IndexKeyRange indexKeyRange,
                             IndexScanSelector scanSelector)
    {
        ArgumentValidation.notNull("indexType", indexType);
        this.index = indexType.index();
        this.reverse = reverse;
        this.indexKeyRange = indexKeyRange;
        this.scanSelector = scanSelector;
    }

    // Class state

    private static final Logger LOG = LoggerFactory.getLogger(IndexScan_Default.class);

    // Object state

    private final Index index;
    private final boolean reverse;
    private final IndexKeyRange indexKeyRange;
    private final IndexScanSelector scanSelector;

    // Inner classes

    private class Execution extends OperatorExecutionBase implements Cursor
    {
        // OperatorExecution interface

        // Cursor interface

        @Override
        public void open(Bindings bindings)
        {
            cursor.open(bindings);
        }

        @Override
        public Row next()
        {
            checkQueryCancelation();
            Row row = cursor.next();
            if (row == null) {
                close();
            } else {
                row.runId(runIdCounter++);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("IndexScan: yield {}", row);
            }
            return row;
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        // Execution interface

        Execution(StoreAdapter adapter)
        {
            super(adapter);
            this.cursor = adapter.newIndexCursor(index, reverse, indexKeyRange, scanSelector);
        }

        // Object state

        private final Cursor cursor;
        private int runIdCounter = 0;
    }
}
