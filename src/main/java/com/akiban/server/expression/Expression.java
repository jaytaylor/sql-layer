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

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.optimizer.explain.Explainer;

public interface Expression {
    boolean nullIsContaminating();
            
    /**
     * <p>Whether this expression, including any child expressions, is a constant.</p>
     * <p>If an expression is constant, it must return {@code false} for both
     * {@link #needsBindings()} and {@link #needsRow()}. Note that the converse is not true: if both "needs"
     * methods return {@code false}, the method may still be non-constant. {@code RAND()} is a good example.</p>
     * @return whether this expression is constant
     */
    boolean isConstant();

    /**
     * <p>Whether this expression requires a binding before it can be evaluated.</p>
     * <p>If this method returns {@code true}, {@link #isConstant()} must return {@code false}</p>
     * @return whether this expression requires a bindings
     */
    boolean needsBindings();

    /**
     * <p>Whether this expression requires a row before it can be evaluated.</p>
     * <p>If this method returns {@code true}, {@link #isConstant()} must return {@code false}</p>
     * @return whether this expression requires a row
     */
    boolean needsRow();

    /**
     * Returns a thread-local, runtime object that reflects this expression. You should not share this object
     * across threads.
     * @return this expression's ExpressionEvaluation
     */
    ExpressionEvaluation evaluation();

    /**
     * The type represented by this expression. {@code this.evaluation().eval()} returns a {@link ValueSource} whose
     * {@link ValueSource#getConversionType()} method must return the same type as returned by this method (or NULL).
     * @return the AkType this expression's runtime instance will eventually have
     */
    // TODO: Should this return ExpressionType? Or is the precision / scale not relevant?
    AkType valueType();
    
    /**
     * 
     * @return The name of the function/arithmetic operator implemented by this
     * expression.
     */
    String name ();
    
    /**
     * 
     * @return the explainer for this expression
     */
    Explainer getExplainer();
}
