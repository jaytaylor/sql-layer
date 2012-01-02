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

import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.ArgList;

import java.util.List;

/** Make an {@link Expression} from a sequence of argument expressions.
 * A composer is normally registered as a named scalar function.
 * Calls to this function will result in the following sequence on the composer:<ol>
 * <li>{@link #argumentTypes} is called with the actual argument types.</li>
 * <li>{@link #composeType} is called with the precise argument types.</li>
 * <li>{@link #compose} is called with the actual argument expressions.</li></ol>
 * @see com.akiban.server.service.functions.Scalar.
 */
public interface ExpressionComposer {
    /**
     * - Adjust input types, if needed
     * - Return the type of a composed expression when passed the given types.
     *
     * If {@link #argumentTypes} adjusted the <code>AkType</code>s,
     * this method can expect to be called with
     * <code>ExpressionType</code>s that are based on those.
     */
    ExpressionType composeType(ArgList argumentTypes) throws StandardException;

    /** Return an expression with the given expressions as its arguments. 
     * If the function has simple type requirements, it can use {@link #argumentTypes}
     * to specify these and then depend on being passed expressions of that type.
     * It can then call type-specific {@link com.akiban.server.types.ValueSource} 
     * methods to get the argument values, after checking
     * {@link com.akiban.server.types.ValueSource#isNull} as necessary.
     * Otherwise, it can use one of the {@link com.akiban.server.types.extract.Extractors}.
     */
    Expression compose(List<? extends Expression> arguments);
}
