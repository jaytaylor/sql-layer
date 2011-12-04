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

package com.akiban.server.expression;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.Shareable;

/**
 * <p>A statement run-time instance of a compiled {@link Expression}. Roughly speaking, ExpressionEvaluation is to
 * Expression as Row is to PhysicalOperators. Note that there is no equivalent to Cursor within the Expression
 * framework.</p>
 *
 * <p>Expressions can come in various flavors: they can require a Row, a Bindings or neither before they can be
 * evaluated. If an Expression requires input, that input is given to its ExpressionEvaluation. E.g.:</p>
 *
 * <pre>
 *  Expression myExpression = someExpression();
 *  assert myExpression.needsRow();
 *  ExpressionEvaluation myEvaluation = myExpression.expressionEvaluation();
 *  myEvaluation.of( myRow );
 *  ValueSource expressionSource = myEvaluation.eval();
 * </pre>
 *
 * <p>A good way to think of the division of labor is that an Expression represents an immutable, compiled expression
 * tree; its ExpressionEvaluation is responsible for accepting input and producing a ValueSource; and the ValueSource
 * is responsible for producing output.</p>
 *
 * <p>Generally speaking, Expressions will belong to some operator specifically charged with evaluating expressions,
 * and the operator's Cursor will generate ExpressionEvaluations per output row. The Cursor will ensure that each
 * evaluation is bound to the Cursor's bindings and the appropriate input row, and it will ensure that the input Row
 * is shared as long as the output Row is. The output Row's {@link Row#eval(int)} method would then delegate to the
 * appropriate ExpressionEvaluation's {@linkplain #eval()}.</p>
 *
 * <p>If an Expression requires input (especially a Row), there are some subtleties:</p>
 * <ul>
 *     <li>it's up to the user of ExpressionEvaluation to know that these inputs are required; failing to provide them
 *     should result in an exception</li>
 *     <li>the ValueSource returned by {@link #eval()} may (and is recommended to be) lazy -- it will evaluate only
 *     when you actually get its value</li>
 *     <li>the ValueSource is also extremely mutable; rebinding the evaluation to a new Row, or letting its bound
 *     Row mutate, will cause the Evaluation to change</li>
 *     <li>none of ExpressionEvaluation's methods ({@link #of(Row)} or {@linkplain #eval()} claim any share on a
 *     Row</li>
 * </ul>
 *
 * <p>This means that you should bind the evaluation, get its ValueSource, and use that ValueSource before re-binding
 * it <em>or</em> letting the Row change (which in most cases means invoking any method on the child Cursor that
 * gave you this Row). If you need to save the evaluation's result, you must copy its value to a field you own. As
 * mentioned above, this would generally be managed by the Cursor that had created the ExpressionEvaluation.</p>
 */
public interface ExpressionEvaluation extends Shareable {
    /**
     * Binds this evaluation to a row
     * @param row the row to be evaluated against
     */
    void of(Row row);

    /**
     * Binds this evaluation to a Bindings
     * @param bindings the bindings to be evaluated against
     */
    void of(Bindings bindings);

    /**
     * Binds this evaluation to a StoreAdapter
     * @param adapter the adapter to use to access storage
     */
    void of(StoreAdapter adapter);

    /**
     * Gets a ValueSource that represents this evaluation. The returned ValueSource is mutable, and may change
     * if you call either {@code of} method <em>or</em> let the bound (input) Bindings or Row change. The ValueSource
     * may be lazily evaluated, too.
     * @return a possibly-lazy, possibly-mutable representation of this evaluation's value
     */
    ValueSource eval();
}
