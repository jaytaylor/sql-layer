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

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.service.FunctionRegistry;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class T3Registry {

    public T3Registry(FunctionRegistry finder) {
        scalars = new InternalScalarsRegistry(finder);
    }

    public T3ScalarsRegistry scalars() {
        return scalars;
    }

    private final T3ScalarsRegistry scalars;

    private static class InternalScalarsRegistry implements T3ScalarsRegistry {
        @Override
        public List<TValidatedOverload> getOverloads(String name) {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public TCast cast(TClass source, TClass target) {
            TCast result = null;
            Map<TClass,TCast> castsByTarget = castsBySource.get(source);
            if (castsByTarget != null)
                result = castsByTarget.get(target);
            if (result == null)
                throw new AkibanInternalException("no cast defined from " + source + " to " + target);
            return result;
        }

        @Override
        public TClassPossibility commonTClass(TClass one, TClass two) {
            throw new UnsupportedOperationException(); // TODO
        }

        private InternalScalarsRegistry(FunctionRegistry finder) {
            overloads = new ArrayList<TValidatedOverload>(finder.overloads());
            Collection<? extends TClass> tClasses = finder.tclasses();
            castsBySource = new HashMap<TClass, Map<TClass, TCast>>(tClasses.size());
            for (TCast cast : finder.casts()) {
                TClass source = cast.sourceClass();
                TClass target = cast.targetClass();
                Map<TClass,TCast> castsByTarget = castsBySource.get(source);
                if (castsByTarget == null) {
                    castsByTarget = new HashMap<TClass, TCast>();
                    castsBySource.put(source, castsByTarget);
                }
                TCast old = castsByTarget.put(target, cast);
                if (old != null)
                    throw new AkibanInternalException("multiple casts defined from " + source + " to " + target);
            }
        }

        private final List<TValidatedOverload> overloads;
        private final Map<TClass,Map<TClass,TCast>> castsBySource;
    }
}
