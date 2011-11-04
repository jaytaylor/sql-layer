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

    /** Return the expected type of the <code>index</code>th argument
     * or <code>null</code> if more than one type is accepted.
     */
    AkType argumentType(int index);
    
    /** Return the type of a composed expression when passed the given
     * types.
     */
    ExpressionType composeType(List<? extends ExpressionType> argumentTypes);
}
