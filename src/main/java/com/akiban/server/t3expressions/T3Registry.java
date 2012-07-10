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
import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.service.FunctionRegistry;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.util.DagChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class T3Registry {

    public T3AggregatesRegistry aggregates() {
        return aggregates;
    }

    public T3Registry(FunctionRegistry finder) {
        scalars = new InternalScalarsRegistry(finder);
        aggregates = new InternalAggregatesRegistry(finder);
    }

    public T3ScalarsRegistry scalars() {
        return scalars;
    }

    private final T3ScalarsRegistry scalars;
    private final T3AggregatesRegistry aggregates;

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
            return result;
        }

        @Override
        public Set<TClass> stronglyCastableTo(TClass tClass) {
            Map<TClass, TCast> castsFrom = strongCastsBySource.get(tClass);
            return castsFrom.keySet();
        }

        private InternalScalarsRegistry(FunctionRegistry finder) {
            Set<? extends TClass> tClasses = new HashSet<TClass>(finder.tclasses());
            castsBySource = new HashMap<TClass, Map<TClass, TCast>>(tClasses.size());

            // First, define the self casts
            for (TClass tClass : tClasses) {
                Map<TClass, TCast> map = new HashMap<TClass, TCast>();
                map.put(tClass, new SelfCast(tClass));
                castsBySource.put(tClass, map);
            }

            // Now the registered casts
            for (TCast cast : finder.casts()) {
                TClass source = cast.sourceClass();
                TClass target = cast.targetClass();
                if (source.equals(target))
                    continue; // TODO remove me!
                Map<TClass,TCast> castsByTarget = castsBySource.get(source);
                TCast old = castsByTarget.put(target, cast);
                if (old != null) {
                    logger.error("CAST({} AS {}): {} replaced by {} ", new Object[]{
                            source, target,  old.getClass(), cast.getClass()
                    });
                    throw new AkibanInternalException("multiple casts defined from " + source + " to " + target);
                }
            }

            strongCastsBySource = createStrongCastsMap(castsBySource);
            checkDag(strongCastsBySource);
        }
        private final Map<TClass,Map<TClass,TCast>> castsBySource;
        private final Map<TClass,Map<TClass,TCast>> strongCastsBySource;
    }

    private static void checkDag(final Map<TClass, Map<TClass, TCast>> castsBySource) {
        DagChecker<TClass> checker = new DagChecker<TClass>() {
            @Override
            protected Set<? extends TClass> initialNodes() {
                return castsBySource.keySet();
            }

            @Override
            protected Set<? extends TClass> nodesFrom(TClass starting) {
                Set<TClass> result = new HashSet<TClass>(castsBySource.get(starting).keySet());
                result.remove(starting);
                return result;
            }
        };
        if (!checker.isDag()) {
            throw new AkibanInternalException("non-DAG detected involving " + checker.getBadNodePath());
        }
    }


    private static Map<TClass,Map<TClass,TCast>> createStrongCastsMap(Map<TClass, Map<TClass, TCast>> castsBySource) {
        Map<TClass,Map<TClass,TCast>> result = new HashMap<TClass, Map<TClass, TCast>>();
        for (Map.Entry<TClass, Map<TClass,TCast>> origEntry : castsBySource.entrySet()) {
            Map<TClass, TCast> strongs = new HashMap<TClass, TCast>();
            for (Map.Entry<TClass,TCast> castByTarget : origEntry.getValue().entrySet()) {
                TCast cast = castByTarget.getValue();
                if (cast.isAutomatic())
                    strongs.put(castByTarget.getKey(), cast);
            }
            assert ! strongs.isEmpty() : origEntry; // self-casts are strong, so there should be at least one entry
            result.put(origEntry.getKey(), strongs);
        }
        return result;
    }

    private static class InternalAggregatesRegistry implements T3AggregatesRegistry {
        @Override
        public Collection<? extends TAggregator> getAggregates(String name) {
            name = name.toLowerCase();
            Collection<? extends TAggregator> aggrs = aggregatorsByName.get(name);
            if (aggrs == null)
                throw new NoSuchFunctionException(name);
            return aggrs;
        }

        private InternalAggregatesRegistry(FunctionRegistry finder) {
            Collection<? extends TAggregator> aggrs = finder.aggregators();
            aggregatorsByName = new HashMap<String, Collection<TAggregator>>(aggrs.size());
            for (TAggregator aggr : aggrs) {
                String name = aggr.name().toLowerCase();
                Collection<TAggregator> values = aggregatorsByName.get(name);
                if (values == null) {
                    values = new ArrayList<TAggregator>(2); // most aggrs don't have many overloads
                    aggregatorsByName.put(name, values);
                }
                values.add(aggr);
            }
        }


        private final Map<String,Collection<TAggregator>> aggregatorsByName;
    }

    private static final Logger logger = LoggerFactory.getLogger(T3Registry.class);

    private static class SelfCast implements TCast {

        @Override
        public boolean isAutomatic() {
            return true;
        }

        @Override
        public Constantness constness() {
            return Constantness.UNKNOWN;
        }

        @Override
        public TClass sourceClass() {
            return tClass;
        }

        @Override
        public TClass targetClass() {
            return tClass;
        }

        @Override
        public TInstance targetInstance(TPreptimeContext context, TPreptimeValue preptimeInput, TInstance specifiedTarget) {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            TInstance srcInst = context.inputTInstanceAt(0);
            TInstance dstInst = context.outputTInstance();
            tClass.selfCast(context, srcInst, source,  dstInst, target);
        }

        SelfCast(TClass tClass) {
            this.tClass = tClass;
        }

        private final TClass tClass;
    }
}
