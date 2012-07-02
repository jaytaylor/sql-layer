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
package com.akiban.server.t3expressions;

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.List;

public interface T3ScalarsRegistry {
    List<TValidatedOverload> getOverloads(String name);

    OverloadResolutionResult get(String name, List<? extends TClass> inputClasses);

    /**
     * Find the registered cast going from source to taret.
     * @param source Type to cast from
     * @param target Type to cast to
     * @return Return matching cast or <tt>null</tt> if none
     */
    TCast cast(TClass source, TClass target);

    /**
     * Returns the common of the two types. For either argument, a <tt>null</tt> value is interpreted as any type.
     * @param one the first type class
     * @param two the other type class
     * @return a wrapper that represents the common class, {@link #NO_COMMON} or {@link #ANY} (the latter only if both
     * inputs are <tt>null</tt>)
     */
    TClassPossibility commonTClass(TClass one, TClass two);


    /**
     * Represents the result that there is <i>no</i> common class.
     */
    public static final TClassPossibility NO_COMMON = new TClassPossibility() {
        @Override
        public boolean isAny() {
            return false;
        }

        @Override
        public boolean isNone() {
            return true;
        }

        @Override
        public TClass get() {
            return null;
        }
    };

    /**
     * Represents the result that <i>any</i> class is a common class.
     */
    public static final TClassPossibility ANY = new TClassPossibility() {
        @Override
        public boolean isAny() {
            return true;
        }

        @Override
        public boolean isNone() {
            return false;
        }

        @Override
        public TClass get() {
            return null;
        }
    };
}
