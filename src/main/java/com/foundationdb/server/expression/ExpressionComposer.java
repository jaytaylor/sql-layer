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

package com.foundationdb.server.expression;

import com.foundationdb.sql.StandardException;

import java.util.List;

/** Make an {@link Expression} from a sequence of argument expressions.
 * A composer is normally registered as a named scalar function.
 * Calls to this function will result in the following sequence on the composer:<ol>
 * <li>{@link #argumentTypes} is called with the actual argument types.</li>
 * <li>{@link #composeType} is called with the precise argument types.</li>
 * <li>{@link #compose} is called with the actual argument expressions.</li></ol>
 * @see com.foundationdb.server.service.functions.Scalar.
 */
public interface ExpressionComposer {
    public static enum NullTreating
    {
        REMOVE_AFTER_FIRST,  // only remove the NULLs if it's the first arg (ELT and CONCAT_WS)
        
        IGNORE,       // The function will treat NULL as if it were any regular value
       
        RETURN_NULL,  // The function will return NULL if at least one of its operands is NULL
        
        REMOVE        // The function essentially ignores NULL, so constant NULL could be removed from the operand-list
                      // The differene between this and IGNORE is that
                                // This doesn't care about NULL, NULL operand(s) will simply be skipped
                                // while IGNORE might do something with NULL
    }
    /**
     * - Adjust input types, if needed
     * - Return the type of a composed expression when passed the given types.
     *
     * If {@link #argumentTypes} adjusted the <code>AkType</code>s,
     * this method can expect to be called with
     * <code>ExpressionType</code>s that are based on those.
     */
    ExpressionType composeType(TypesList argumentTypes) throws StandardException;

    /** Return an expression with the given expressions as its arguments. 
     * If the function has simple type requirements, it can use {@link #argumentTypes}
     * to specify these and then depend on being passed expressions of that type.
     * It can then call type-specific {@link com.foundationdb.server.types.ValueSource} 
     * methods to get the argument values, after checking
     * {@link com.foundationdb.server.types.ValueSource#isNull} as necessary.
     * Otherwise, it can use one of the {@link com.foundationdb.server.types.extract.Extractors}.
     *
     * typesList.size() should be (arguments.size() + 1), where the last element 
     * in typesList is the return type, and the rest is the arguments' type
     * 
     * @param arguments
     * @param typesList: arguments' type AND the return type.
     * @return 
     */
    Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList);
    
    /**
     * 
     * @return IGNORE 
     *                  If NULL operand(s) will simply be ignored
     *         RETURN_NULL
     *                  If NULL operand(s) will make this expression return NULL
     *         REMOVE
     *                  If NULL expression(s) should be removed (optimisation purpose)
     *         
     */
    NullTreating getNullTreating();
}
