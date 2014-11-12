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

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.qp.util.HashTable;
import com.foundationdb.util.tap.InOutTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;


class Using_HashTable extends Operator
{
    // Object interface

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    // Operator interface

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes)
    {
        hashInput.findDerivedTypes(derivedTypes);
        joinedInput.findDerivedTypes(derivedTypes);
    }

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor)
    {
        return new Execution(context, joinedInput.cursor(context, bindingsCursor));
    }

    @Override
    public List<Operator> getInputOperators()
    {
        return Arrays.asList(hashInput, joinedInput);
    }

    @Override
    public String describePlan()
    {
        return String.format("%s\n%s", describePlan(hashInput), describePlan(joinedInput));
    }

    public Using_HashTable(Operator hashInput,
                           RowType hashedRowType,
                           List<TPreparedExpression> comparisonFields,
                           int tableBindingPosition,
                           Operator joinedInput,
                           List<TComparison> tComparisons,
                           List<AkCollator> collators)
    {
        ArgumentValidation.notNull("hashInput", hashInput);
        ArgumentValidation.notNull("hashedRowType", hashedRowType);
        ArgumentValidation.notNull("comparisonFields", comparisonFields);
        ArgumentValidation.isGTE("comparisonFields", comparisonFields.size(), 1);
        ArgumentValidation.isLTE("comparisonFields", comparisonFields.size(), hashedRowType.nFields());
        ArgumentValidation.notNull("joinedInput", joinedInput);

        this.hashInput = hashInput;
        this.hashedRowType = hashedRowType;
        this.tableBindingPosition = tableBindingPosition;
        this.joinedInput = joinedInput;
        this.tComparisons = tComparisons;
        this.collators = collators;
        this.comparisonFields = comparisonFields;

    }


    // Class state

    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Using_HashTable open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Using_HashTable next");
    private static final Logger LOG = LoggerFactory.getLogger(Using_HashTable.class);

    // Object state

    private final Operator hashInput;
    private final RowType hashedRowType;
    private final int tableBindingPosition;
    private final Operator joinedInput;
    private final List<AkCollator> collators;
    private final List<TComparison> tComparisons;
    private final List<TPreparedExpression> comparisonFields;


    @Override
    public CompoundExplainer getExplainer(ExplainContext context) {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        atts.put(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(tableBindingPosition));
        atts.put(Label.INPUT_OPERATOR, hashInput.getExplainer(context));
        atts.put(Label.INPUT_OPERATOR, joinedInput.getExplainer(context));
        for (TPreparedExpression field : comparisonFields) {
            atts.put(Label.EXPRESSIONS, field.getExplainer(context));
        }
        return new CompoundExplainer(Type.HASH_JOIN, atts);
    }

    // Inner classes

    private class Execution extends ChainedCursor
    {
        // Cursor interface
        private final List<TEvaluatableExpression> evaluatableComparisonFields = new ArrayList<>();

        @Override
        public void open()
        {
            TAP_OPEN.in();
            try {
                // Usually super.open called first, but needs to be done
                // opposite order here to allow Using_HashFilter access
                // to the filled HashTable in the bindings. 
                HashTable hashTable = buildHashTable();
                bindings.setHashTable(tableBindingPosition, hashTable);
                super.open();
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
                Row output = input.next();
                if (LOG_EXECUTION) {
                    LOG.debug("Using_HashTable: yield {}", output);
                }
                return output;
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        @Override
        public void close()
        {
            if (bindings != null) {
                bindings.setHashTable(tableBindingPosition, null);
            }
            super.close();
        }

        // Execution interface

        Execution(QueryContext context, Cursor input)
        {
            super(context, input);
            for(TPreparedExpression comparisonField : comparisonFields){
                evaluatableComparisonFields.add(comparisonField.build());
            }
        }

        // For use by this class

        private HashTable  buildHashTable() {
            QueryBindingsCursor bindingsCursor = new SingletonQueryBindingsCursor(bindings);
            Cursor loadCursor = hashInput.cursor(context, bindingsCursor);
            loadCursor.openTopLevel();
            Row row;
            HashTable hashTable= new HashTable();
            hashTable.setRowType(hashedRowType);
            hashTable.setTComparisons(tComparisons);
            hashTable.setCollators(collators);
            while ((row = loadCursor.next()) != null) {
                assert(row.rowType() == hashedRowType) : row;
                hashTable.put(row, evaluatableComparisonFields, bindings);
            }
            loadCursor.closeTopLevel();
            return hashTable;
        }
     }
}
