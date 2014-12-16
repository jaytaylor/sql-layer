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
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.AggregatedRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.explain.*;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.aggr.MCount;
import com.foundationdb.server.types.value.*;
import com.foundationdb.util.ArgumentValidation;
import com.foundationdb.util.tap.InOutTap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**

 <h1>Overview</h1>

 Aggregation_Partial applies a partial aggregation to rows. By partial
 we mean that if the aggregation contains a GROUP BY, output rows will
 be streamed out as soon as a change is detected in the GROUP BY
 columns; no attempt is made to sort or hash values. This means that if
 the incoming rows are not sorted by their GROUP BY columns, this
 operator may output more than one aggregation per GROUP BY value. If
 this happens, each aggregation happens independent of the others.

 <h1>Arguments</h1>

 <ul>

 <li><b>input:</b> the input operator

 <li><b>inputsIndex:</b> the first index of the input rows that
 represents an input; indexes before this are GROUP BY
 fields. Required: <i>0 <= inputsIndex < input.rowType().nFields()</i>

 <li><b>aggregatorFactory:</b> a mapping of <i>String -> Aggregator</i>

 <li><b>aggregatorNames:</b> a list of aggregator function to be given
 to the factory. Required: <i>inputsIndex + aggregatorNames.size() ==
 input.rowType().nFields()</i>

 </ul>

 <h1>Behavior</h1>

 This operator takes as input a row which is interpreted as having two
 sections: a GROUP BY section and an inputs section. These are
 delimited by the <i>inputsIndex</i> argument, which specifies the
 index of the first input. The operator also has a list of aggregator
 functions (specified by the <i>aggregatorNames</i> list), one per
 input.

 For each input row of type <i>input.rowType()</i>, the
 Aggregation_Partial's cursor applies each input to its appropriate
 aggregator. When the cursor notices a change in any of the GROUP BY
 columns, or when the input cursor is finished, the aggregation cursor
 outputs a row with the GROUP BY columns and the result of each
 aggregation.

 <h2>Example</h2>

 Let's say we have a table describing various SKUs in warehouses. Each
 warehouse has a category for SKUs, but these categories are not unique
 among all warehouses. We want to get the sum, min and max price of
 each category in each warehouse.

 <i>SELECT warehouse, product_category, SUM(price), MIN(price), MAX(price) FROM products GROUP BY warehouse, product_category</i>

 The input rows to the Aggregation_Partial cursor would be something like:

 <table>
 <tr><td> warehouse </td><td> product_category </td><td> price </td><td> price </td><td> price </td><td> notes </td></tr>
 <tr><td> 0001 </td><td> AAA </td><td>  5.00 </td><td>  5.00 </td><td>  5.00 </td><td> sku 1 </td></tr>
 <tr><td> 0001 </td><td> AAA </td><td> 10.00 </td><td> 10.00 </td><td> 10.00 </td><td> sku 2 </td></tr>
 <tr><td> 0002 </td><td> AAA </td><td>  7.00 </td><td>  7.00 </td><td>  7.00 </td><td> sku 3 </td></tr>
 <tr><td> 0002 </td><td> AAA </td><td>  3.00 </td><td>  3.00 </td><td>  3.00 </td><td> sku 4 </td></tr>
 <tr><td> 0002 </td><td> BBB </td><td> 11.00 </td><td> 11.00 </td><td> 11.00 </td><td> sku 5 </td></tr>
 </table>

 The <i>inputsIndex</i> here is <i>2</i>. Note that the "price" column has been repeated three times,
 once for each aggregate function. In this case, the input rows are already ordered by their GROUP BY columns;
 if we knew this were the case (due to another operator's ordering), this partial aggregation would also be a
 full aggregation.

 The output rows would look like:

 <table>
 <tr><td> warehouse </td><td> product_category </td><td> SUM(price) </td><td> MIN(price) </td><td> MAX(price) </td></tr>
 <tr><td> 0001 </td><td> AAA </td><td> 15.00 </td><td>  5.00 </td><td> 10.00 </td></tr>
 <tr><td> 0002 </td><td> AAA </td><td> 10.00 </td><td>  3.00 </td><td>  7.00 </td></tr>
 <tr><td> 0002 </td><td> BBB </td><td> 11.00 </td><td> 11.00 </td><td> 11.00 </td></tr>
 </table>

 <h2>Notes</h2>

 If there are no input rows, behavior depends on
 the <i>inputIndex</i>. If it is 0 (no GROUP BY) columns, this
 Aggregation_Partial will not output any rows. If <i>inputIndex > 0</i>
 (there is a GROUP BY), a single row will be outputted with all NULL
 values. This is due to the SQL spec.

 This operator cannot do something like an average directly. Instead,
 the operator tree would use this operator to get a SUM and COUNT of
 rows, and another operator would be responsible for dividing them to
 get the average.

 <h1>Output</h1>

 All input rows are swallowed. Output rows are as described above. All
 rows from the incoming operator with a type other
 than <i>input.rowType()</i> are passed through unchanged.

 <h1>Assumptions</h1>

 None; but if you want a full aggregation, it is up to you to pre-order
 the rows by their GROUP BY columns. If an aggregator is not amenable
 to this piecemeal aggregation, you should not use it with a partial
 aggregation (none of the aggregators we plan on writing have this
 problem).

 <h1>Performance</h1>

 Partially dictated by aggregators, though expected to be
 minimal. Comparison of GROUP BY columns is O(N).

 <h1>Memory requirements</h1>

 One row and one set of grouping column values.

 */

final class Aggregate_Partial extends Operator
{

    // Operator interface

    @Override
    protected Cursor cursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
        return new AggregateCursor(context, bindingsCursor);
    }

    @Override
    public void findDerivedTypes(Set<RowType> derivedTypes) {
        inputOperator.findDerivedTypes(derivedTypes);
        derivedTypes.add(outputType);
    }

    @Override
    public List<Operator> getInputOperators() {
        return Collections.singletonList(inputOperator);
    }

    @Override
    public RowType rowType() {
        return outputType;
    }

    // AggregationOperator interface

    public Aggregate_Partial(Operator inputOperator,
                             RowType inputRowType,
                             int inputsIndex,
                             List<? extends TAggregator> aggregatorFactories,
                             List<? extends TInstance> pAggrTypes,
                             List<Object> options) {
        this.inputOperator = inputOperator;
        this.inputRowType = inputRowType;
        this.inputsIndex = inputsIndex;
        this.outputType = inputRowType.schema().newAggregateType(inputRowType, inputsIndex, pAggrTypes);
        this.pAggrs = aggregatorFactories;
        this.pAggrTypes = pAggrTypes;
        this.options = options;
        validate();
        
    }

    // Object interface

    @Override
    public String toString() {
        if (inputsIndex == 0) {
            return String.format("Aggregation(without GROUP BY: %s)", aggrsToString());
        }
        if (inputsIndex == 1) {
            return String.format("Aggregation(GROUP BY 1 field, then: %s)", aggrsToString());
        }
        return String.format("Aggregation(GROUP BY %d fields, then: %s)", inputsIndex, aggrsToString());
    }

    private String aggrsToString() {
        int pAggersLen = pAggrs.size();
        StringBuilder sb = new StringBuilder(pAggersLen * 6); // guess at the size, doesn't matter much
        sb.append('[');
        for (int i = 0; i < pAggersLen; ++i) {
            TAggregator aggregator = pAggrs.get(i);
            sb.append(aggregator);
            if (! (aggregator instanceof MCount)) {
                sb.append(rowType().typeAt(i + inputsIndex).typeClass().name().unqualifiedName());
            }
            if ( (i+1) < pAggersLen)
                sb.append(", ");
        }
        sb.append(']');
        return sb.toString();
    }

    // private methods

    private void validate() {
        if (inputOperator == null || inputRowType == null || outputType == null)
            throw new NullPointerException();
        ArgumentValidation.isBetween("inputsIndex", 0, inputsIndex, inputRowType.nFields()+1);
        int aggersSize;
        aggersSize = pAggrs.size();
        if (pAggrTypes.size() != pAggrs.size())
            throw new IllegalArgumentException("aggregators and aggregator types mismatch in size");
        if (inputsIndex + aggersSize != inputRowType.nFields()) {
            throw new IllegalArgumentException(
                    String.format("inputsIndex(=%d) + aggregatorNames.size(=%d) != inputRowType.nFields(=%d)",
                            inputsIndex, aggersSize, inputRowType.nFields()
            ));
        }
    }
    
    // class state
    
    private static final InOutTap TAP_OPEN = OPERATOR_TAP.createSubsidiaryTap("operator: Aggregate_Partial open");
    private static final InOutTap TAP_NEXT = OPERATOR_TAP.createSubsidiaryTap("operator: Aggregate_Partial next");
    private static final Logger LOG = LoggerFactory.getLogger(Aggregate_Partial.class);

    // object state

    private final Operator inputOperator;
    private final RowType inputRowType;
    private final AggregatedRowType outputType;
    private final int inputsIndex;
    private final List<? extends TInstance> pAggrTypes;
    private final List<? extends TAggregator> pAggrs;
    private final List<Object> options; // currently only used by GROUP_CONCAT, meaning the optional SEPARATOR string

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        Attributes atts = new Attributes();
        atts.put(Label.NAME, PrimitiveExplainer.getInstance(getName()));
        for (TAggregator agg : pAggrs)
            atts.put(Label.AGGREGATORS, PrimitiveExplainer.getInstance(agg.displayName().toUpperCase()));
        atts.put(Label.GROUPING_OPTION, PrimitiveExplainer.getInstance(inputsIndex));
        atts.put(Label.INPUT_OPERATOR, inputOperator.getExplainer(context));
        atts.put(Label.INPUT_TYPE, inputRowType.getExplainer(context));
        atts.put(Label.OUTPUT_TYPE, outputType.getExplainer(context));
        return new CompoundExplainer(Type.AGGREGATE, atts);
    }

    // nested classes

    private class AggregateCursor extends ChainedCursor
    {

        // Cursor interface

        @Override
        public void open() {
            TAP_OPEN.in();
            try {
                super.open();
            } finally {
                TAP_OPEN.out();
            }
        }

        @Override
        public Row next() {
            if (TAP_NEXT_ENABLED) {
                TAP_NEXT.in();
            }
            try {
                // CursorLifecycle.checkIdleOrActive(this);
                checkQueryCancelation();
                if (CURSOR_LIFECYCLE_ENABLED) {
                    CursorLifecycle.checkIdleOrActive(this);
                }
                if (isIdle()) {
                    if (LOG_EXECUTION) {
                        LOG.debug("Aggregate_Partial: null");
                    }
                    return null;
                }
                
                while (true) {
                    Row input = nextInput();
                    Row output;
                    if (input == null) {
                        if (everSawInput) {
                            output = createOutput();
                        }
                        else if (noGroupBy()) {
                            output = createEmptyOutput();
                        }
                        else {
                            output = null;
                        }
                        if (LOG_EXECUTION) {
                            LOG.debug("Aggregate_Partial: yield {}", output);
                        }
                        setIdle();
                        return output;
                    }
                    if (!(input.rowType() == inputRowType)) {
                        if (LOG_EXECUTION) {
                            LOG.debug("Aggregate_Partial: yield {}", input);
                        }
                        return input; // pass through
                    }
                    everSawInput = true;
                    if (outputNeeded(input)) {
                        saveInput(input); // save this input for the next time this method is invoked
                        output = createOutput();
                        if (LOG_EXECUTION) {
                            LOG.debug("Aggregate_Partial: yield {}", output);
                        }
                        return output;
                    }
                    aggregate(input);
                }
            } finally {
                if (TAP_NEXT_ENABLED) {
                    TAP_NEXT.out();
                }
            }
        }

        // for use in this class

        private void aggregate(Row input) {
            for (int i=0; i < pAggrs.size(); ++i) {
                TAggregator aggregator = pAggrs.get(i);
                int inputIndex = i + inputsIndex;
                TInstance inputType = input.rowType().typeAt(inputIndex);
                ValueSource inputSource = input.value(inputIndex);
                aggregator.input(inputType, inputSource, pAggrTypes.get(i), pAggrsStates.get(i), options.get(i));
            }
        }

        private Row createOutput() {
            ValuesHolderRow outputRow = newOutputRow();
            for(int i = 0; i < inputsIndex; ++i) {
                Value value = outputRow.valueAt(i);
                Value key = keyValues.get(i);
                ValueTargets.copyFrom(key, value);
            }
            for (int i = inputsIndex; i < inputRowType.nFields(); ++i) {
                Value value = outputRow.valueAt(i);
                int aggregatorIndex = i - inputsIndex;
                Value aggregatorState = pAggrsStates.get(aggregatorIndex);
                if (aggregatorState.hasAnyValue())
                    ValueTargets.copyFrom(aggregatorState, value);
                else
                    pAggrs.get(aggregatorIndex).emptyValue(value);
                aggregatorState.unset();
            }
            return outputRow;
        }

        private Row createEmptyOutput() {
            assert noGroupBy() : "shouldn't be creating null output row when I have a grouping";
            ValuesHolderRow outputRow = newOutputRow();
            for (int i = 0; i < outputRow.rowType().nFields(); ++i) {
                pAggrs.get(i).emptyValue(outputRow.valueAt(i));
            }
            return outputRow;
        }

        private boolean noGroupBy() {
            return inputsIndex == 0;
        }

        private boolean outputNeeded(Row givenInput) {
            if (noGroupBy()) {
                this.gatheringRows = false;
                return false;   // no GROUP BYs, so aggregate until givenInput is null
            }

            // check for any changes to keys
            // Coming into this code, we're either RUNNING (within a GROUP BY run) or OPENING (about to start
            // a new run).
            if (gatheringRows) {
                for (int i = 0; i < keyValues.size(); ++i) {
                    ValueTargets.copyFrom(givenInput.value(i), keyValues.get(i));
                }
                gatheringRows = false;
                return false;
            }
            else {
                // If any keys are different, switch mode to OPENING and return true; else return false.
                for (int i = 0; i < keyValues.size(); ++i) {
                    Value key = keyValues.get(i);
                    ValueSource input = givenInput.value(i);
                    if (!TClass.areEqual(key, input)) {
                        gatheringRows = true;
                        return true;
                    }
                }
                return false;
            }
        }

        private Row nextInput() {
            final Row result;
            if (holder != null) {
                result = holder;
                holder = null;
            }
            else {
                result = input.next();
            }
            return result;
        }

        private void saveInput(Row input) {
            assert holder == null : holder;
            holder = input;
        }

        private ValuesHolderRow newOutputRow() {
            return new ValuesHolderRow(outputType);
        }

        // AggregateCursor interface

        private AggregateCursor(QueryContext context, QueryBindingsCursor bindingsCursor) {
            super(context, inputOperator.cursor(context, bindingsCursor));
            keyValues = new ArrayList<>(inputsIndex);
            for (int i = 0; i < inputsIndex; ++i) {
                keyValues.add(new Value(outputType.typeAt(i)));
            }
            int nAggrs = pAggrs.size();
            pAggrsStates = new ArrayList<>(nAggrs);
            for (int i = 0; i < nAggrs; i++) {
                Value state = new Value(pAggrTypes.get(i));
                pAggrsStates.add(state);
            }
        }


        // object state

        private final List<Value> keyValues;
        private final List<Value> pAggrsStates;
        private Row holder;
        private boolean everSawInput = false;
        private boolean gatheringRows = true;
    }

}
