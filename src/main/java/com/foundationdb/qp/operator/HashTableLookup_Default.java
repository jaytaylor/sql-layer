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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.*;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedBoundField;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.HashTable;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class HashTableLookup_Default extends Operator
{
    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
    }

    public HashTableLookup_Default(List<AkCollator> collators,
                                   List<TPreparedExpression> outerComparisonFields,
                                   int hashTableBindingPosition
    )
    {
        ArgumentValidation.notNull("outerComparisonFields", outerComparisonFields);
        ArgumentValidation.isGTE("outerComparisonFields", outerComparisonFields.size(), 1);
        int i = 0;
        for(TPreparedExpression comparisonField : outerComparisonFields){
            evaluatableComparisonFields.add(comparisonField.build());
        }

        this.collators = collators;

        this.hashTableBindingPosition = hashTableBindingPosition;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: HashTableLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: HashTableLookup_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state


    private final int hashTableBindingPosition;
    private RowType hashedRowType;

    private final List<AkCollator> collators;
    private final List<TEvaluatableExpression> evaluatableComparisonFields = new ArrayList<>();

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        return new CompoundExplainer(Type.HASH_JOIN, atts);
    }

    private class Execution extends OperatorCursor
    {

        @Override
        public void open(){
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);

                hashTable = bindings.getHashTable(hashTableBindingPosition);
                hashedRowType = hashTable.getRowType();
                innerRowList = hashTable.getMatchingRows(null, evaluatableComparisonFields, collators, bindings);
                innerRowListPosition = 0;
                closed = false;
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next()
        {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                Row next = null;
                if(innerRowListPosition < innerRowList.size()) {
                    next = innerRowList.get(innerRowListPosition++);
                    assert(next.rowType().equals(hashedRowType));
                }
                if (LOG_EXECUTION) {
                    LOG.debug("HashJoin: yield {}", next);
                }
                return next;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            CursorLifecycle.checkIdleOrActive(this);
            if (!closed) {
                closed = true;
            }
        }

        @Override
        public void closeBindings() {
            bindingsCursor.closeBindings();
        }

        @Override
        public void destroy()
        {
            close();
            destroyed = true;
        }

        @Override
        public boolean isIdle()
        {
            return closed;
        }

        @Override
        public boolean isActive()
        {
            return !closed;
        }

        @Override
        public void openBindings() {
            bindingsCursor.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
             bindings = bindingsCursor.nextBindings();
            return bindings;
        }

        @Override
        public boolean isDestroyed()
        {
            return destroyed;
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            bindingsCursor.cancelBindings(bindings);
        }

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context);
            MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
            this.bindingsCursor = multiple;
        }
        // Cursor interface
        protected HashTable hashTable;
        private boolean closed = true;
        private final QueryBindingsCursor bindingsCursor;
        private List<Row> innerRowList;
        private int innerRowListPosition = 0;
        private QueryBindings bindings;
        private boolean destroyed = false;
    }
}