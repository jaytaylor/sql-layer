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
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.qp.util.HashTable;
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

    public HashTableLookup_Default(RowType hashedRowType,
                                   List<TPreparedExpression> outerComparisonFields,
                                   int hashTableBindingPosition
    )
    {
        ArgumentValidation.notNull("hashedRowType", hashedRowType);
        ArgumentValidation.notNull("outerComparisonFields", outerComparisonFields);
        ArgumentValidation.isGTE("outerComparisonFields", outerComparisonFields.size(), 1);

        this.hashedRowType = hashedRowType;
        this.hashTableBindingPosition = hashTableBindingPosition;
        this.outerComparisonFields = outerComparisonFields;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: HashTableLookup_Default open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: HashTableLookup_Default next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final int hashTableBindingPosition;
    private final RowType hashedRowType;
    List<TPreparedExpression> outerComparisonFields;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(hashTableBindingPosition));
        for (TPreparedExpression field : outerComparisonFields) {
            atts.put(Label.EXPRESSIONS, field.getExplainer(context));
        }
        return new CompoundExplainer(Type.HASH_JOIN, atts);
    }

    private class Execution extends LeafCursor
    {

        @Override
        public void open(){
            TAP_OPEN.in();
            try {
                super.open();
                hashTable = bindings.getHashTable(hashTableBindingPosition);
                assert (hashedRowType == hashTable.getRowType()) : hashTable;
                innerRowList = hashTable.getMatchingRows(null, evaluatableComparisonFields, bindings);
                innerRowListPosition = 0;
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

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context,  new MultipleQueryBindingsCursor(bindingsCursor));
            for (TPreparedExpression comparisonField : outerComparisonFields) {
                evaluatableComparisonFields.add(comparisonField.build());
            }
        }
        // Cursor interface
        protected HashTable hashTable;
        private List<Row> innerRowList;
        private int innerRowListPosition = 0;
        private final List<TEvaluatableExpression> evaluatableComparisonFields = new ArrayList<>();

    }
}