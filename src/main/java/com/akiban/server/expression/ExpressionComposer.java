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

import java.util.List;

public interface ExpressionComposer {
    /** Return an expression with the given expressions as its arguments. 
     */
    Expression compose(List<? extends Expression> arguments);

    /** Given arguments of the specified types, adjust them for any
     * type requirements, including from promotion based on other
     * argument types. {@link #compose} and {@link #composeType} can then
     * expect to receive those types and throw an exception if they do not.
     */
    void argumentTypes(List<AkType> argumentTypes);
    
    /** Return the type of a composed expression when passed the given
     * types.
     */
    ExpressionType composeType(List<? extends ExpressionType> argumentTypes);
}
