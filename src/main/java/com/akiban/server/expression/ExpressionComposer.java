/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.expression;

import com.akiban.sql.StandardException;

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
     * It can then call type-specific {@link com.akiban.server.types.ValueSource} 
     * methods to get the argument values, after checking
     * {@link com.akiban.server.types.ValueSource#isNull} as necessary.
     * Otherwise, it can use one of the {@link com.akiban.server.types.extract.Extractors}.
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
