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
import com.foundationdb.qp.rowtype.*;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;
import com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

class HashJoin extends Operator
{
    @Override
    public String toString()
    {
        return "HashJoin";
    }

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, bindingsCursor);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        outerInputOperator.findDerivedTypes(derivedTypes);
        innerInputOperator.findDerivedTypes(derivedTypes);
    }

    @Override
    public List<Operator> getInputOperators()
    {
        List<Operator> result = new ArrayList<>(2);
        result.add(innerInputOperator);
        result.add(outerInputOperator);
        return result;
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(outerInputOperator), describePlan(innerInputOperator));
    }

    public HashJoin(Operator outerInputOperator,
                    Operator innerInputOperator,
                    //List<? extends TPreparedExpression> tFields,
                    List<AkCollator> collators,
                    int outerComparisonFields[],
                    int innerComparisonFields[])
    {
        ArgumentValidation.notNull("outerInputOperator", outerInputOperator);
        ArgumentValidation.notNull("innerInputOperator", innerInputOperator);
        ArgumentValidation.isGTE("outerOrderingFields", outerComparisonFields.length, 1);
        ArgumentValidation.isGTE("innerOrderingFields", innerComparisonFields.length, 1);
        ArgumentValidation.isSame("innerComparisonFields", innerComparisonFields, "outerComparisonFields", outerComparisonFields);
        //ArgumentValidation.notNull("Fields", tFields);
        //ArgumentValidation.isGT("fields.size()", tFields.size(), 0);
        this.outerInputOperator = outerInputOperator;
        this.innerInputOperator = innerInputOperator;
        //this.tFields = tFields;
        this.collators = collators;
        this.outerComparisonFields = outerComparisonFields;
        this.innerComparisonFields = innerComparisonFields;
    }

    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: intersect_Ordered open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: intersect_Ordered next");
    private static final Logger LOG = LoggerFactory.getLogger(Intersect_Ordered.class);

    // Object state

    private final Operator outerInputOperator;
    private final Operator innerInputOperator;
    private final int outerComparisonFields[];
    private final int innerComparisonFields[];
   // private final List<? extends TPreparedExpression> tFields;
    private final List<AkCollator> collators;

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        //atts.put(Label.NUM_COMPARE, PrimitiveExplainer.getInstance(tFields.size()));
        atts.put(Label.INPUT_OPERATOR, outerInputOperator.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, innerInputOperator.getExplainer(context));
        return new CompoundExplainer(Type.HASH_JOIN, atts);
    }

    private class Execution<E> extends OperatorCursor
    {

        @Override
        public void open(){
            TAP_OPEN.in();
            try {
                CursorLifecycle.checkIdle(this);
                outerInput.open();
                innerInput.open();
                buildHashTable();
                innerInput.close();
                nextOuterRow();
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
                while (!closed && next == null && outerRow != null) {
                    if (innerRowList.isEmpty()) {
                        Integer keyRow = hashProjectedRow(outerRow, outerComparisonFields);
                        innerRowList = multimap.get(keyRow);
                        innerRowListPosition = 0;
                        if (innerRowList.isEmpty()) {
                            nextOuterRow();
                        }
                    }
                    if (!innerRowList.isEmpty()) {
                        innerRow = innerRowList.get(innerRowListPosition++);
                        if (innerRowListPosition == innerRowList.size()) {
                            innerRowList = null;
                        }
                        next = joinRows(innerRow, outerRow);
                    }
                }
                if (LOG_EXECUTION) {
                    LOG.debug("Hash_Join: yield {}", next);
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
                outerRow = null;
                innerRow = null;
                outerInput.close();
                innerInput.close();
                closed = true;
            }
        }

        @Override
        public void closeBindings() {
            bindingsCursor.closeBindings();
            outerInput.closeBindings();
            innerInput.closeBindings();
        }

        @Override
        public void destroy()
        {
            close();
            outerInput.destroy();
            innerInput.destroy();
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
        public boolean isDestroyed()
        {
            assert outerInput.isDestroyed() == innerInput.isDestroyed();
            return innerInput.isDestroyed();
        }

        @Override
        public void openBindings() {
            bindingsCursor.openBindings();
            outerInput.openBindings();
            innerInput.openBindings();
        }

        @Override
        public QueryBindings nextBindings() {
            QueryBindings bindings = bindingsCursor.nextBindings();
            QueryBindings other = outerInput.nextBindings();
            assert (bindings == other);
            other = innerInput.nextBindings();
            assert (bindings == other);
            return bindings;
        }

        @Override
        public void cancelBindings(QueryBindings bindings) {
            outerInput.cancelBindings(bindings);
            innerInput.cancelBindings(bindings);
            bindingsCursor.cancelBindings(bindings);
        }

        // For use by this class

        private void nextOuterRow()
        {
            Row row = outerInput.next();
            outerRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("hash_join: outer {}", row);
            }
        }

        private void nextInnerRow()
        {
            Row row = innerInput.next();
            innerRow = row;
            if (LOG_EXECUTION) {
                LOG.debug("hash_join: inner {}", row);
            }
        }


        // Execution interface

        Execution(QueryContext context, QueryBindingsCursor bindingsCursor)
        {
            super(context);
            MultipleQueryBindingsCursor multiple = new MultipleQueryBindingsCursor(bindingsCursor);
            this.bindingsCursor = multiple;
            this.outerInput = outerInputOperator.cursor(context, multiple.newCursor());
            this.innerInput = innerInputOperator.cursor(context, multiple.newCursor());
        }
        // Cursor interface
        
        private void  buildHashTable() {
            nextInnerRow();
            while (innerRow != null) {
                Integer key = hashProjectedRow(innerRow, innerComparisonFields);
                multimap.put(key, innerRow);
                nextInnerRow();
            }
        }

        private Integer hashProjectedRow(Row row, int comparisonFields[])
        {
            int hash = 0;
            for (int f = 0; f < comparisonFields.length; f++) {
                ValueSource columnValue= row.value(comparisonFields[f])   ;
                hash = hash ^ ValueSources.hash(columnValue, collators.get(f));
            }
            return hash;
        }

        /** Personal row used to combine the inner and outer row before returning */
        protected Row joinRows(Row row1, Row row2){
            JoinedRowType joinedRowType = new JoinedRowType(row1.rowType().schema(), 1, row1.rowType(), row2.rowType());
            return new CompoundRow(joinedRowType, row1, row2);
        }

        public class JoinedRowType extends CompoundRowType {

            public JoinedRowType(Schema schema, int typeID, RowType first, RowType second) {
                super(schema, typeID, first, second);
            }
        }

        protected final Cursor outerInput;
        protected final Cursor innerInput;
        protected ArrayListMultimap<Integer, Row> multimap = ArrayListMultimap.create();
        private boolean closed = true;
        private final QueryBindingsCursor bindingsCursor;
        private Row outerRow;
        private Row innerRow;
        private List<Row> innerRowList;
        private int innerRowListPosition;



    }
}
