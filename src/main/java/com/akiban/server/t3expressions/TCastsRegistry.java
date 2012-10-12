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
import com.akiban.server.types3.TCastIdentifier;
import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.TStrongCasts;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.service.InstanceFinder;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.util.DagChecker;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public final class TCastsRegistry {

    public TCast cast(TClass source, TClass target) {
        return cast(castsBySource, source, target);
    }

    public Set<TClass> stronglyCastableFrom(TClass tClass) {
        Map<TClass, TCast> castsFrom = strongCastsBySource.get(tClass);
        return castsFrom.keySet();
    }

    public boolean isStrong(TCast cast) {
        TClass source = cast.sourceClass();
        TClass target = cast.targetClass();
        return stronglyCastableFrom(source).contains(target);
    }

    Collection<Map<TClass, TCast>> castsBySource() {
        return castsBySource.values();
    }

    TCastsRegistry(Collection<? extends TClass> tClasses, InstanceFinder finder)
    {
        castsBySource = createCasts(tClasses, finder);
        createDerivedCasts(castsBySource, finder);
        deriveCastsFromVarchar();
        strongCastsBySource = createStrongCastsMap(castsBySource, finder);
        checkDag(strongCastsBySource);
    }

    private final Map<TClass,Map<TClass,TCast>> castsBySource;
    private final Map<TClass,Map<TClass,TCast>> strongCastsBySource;

    private static final Logger logger = LoggerFactory.getLogger(TCastsRegistry.class);
    private static final Comparator<TCastIdentifier> tcastIdentifierComparator = new Comparator<TCastIdentifier>() {
        @Override
        public int compare(TCastIdentifier o1, TCastIdentifier o2) {
            String o1Str = o1.toString();
            String o2Str = o2.toString();
            return o1Str.compareTo(o2Str);
        }
    };

    // package-local; also used in testing

    static Map<TClass, Map<TClass, TCast>> createCasts(Collection<? extends TClass> tClasses,
                                                       InstanceFinder finder) {
        Map<TClass, Map<TClass, TCast>> localCastsMap = new HashMap<TClass, Map<TClass, TCast>>(tClasses.size());

        // First, define the self casts
        for (TClass tClass : tClasses) {
            Map<TClass, TCast> map = new HashMap<TClass, TCast>();
            map.put(tClass, new SelfCast(tClass));
            localCastsMap.put(tClass, map);
        }

        Set<TCastIdentifier> duplicates = new TreeSet<TCastIdentifier>(tcastIdentifierComparator);

        // Next, to/from varchar
        for (TClass tClass : tClasses) {
            putCast(localCastsMap, tClass.castToVarchar(), duplicates);
            putCast(localCastsMap, tClass.castFromVarchar(), duplicates);
        }

        // Now the registered casts
        for (TCast cast : finder.find(TCast.class)) {
            putCast(localCastsMap, cast, duplicates);
        }

        if (!duplicates.isEmpty())
            throw new AkibanInternalException("duplicate casts found for: " + duplicates);
        return localCastsMap;
    }

    static Map<TClass, Map<TClass, TCast>> createStrongCastsMap(Map<TClass, Map<TClass, TCast>> castsBySource,
                                                                final Set<TCastIdentifier> strongCasts) {
        Map<TClass,Map<TClass,TCast>> result = new HashMap<TClass, Map<TClass, TCast>>();
        for (Map.Entry<TClass, Map<TClass,TCast>> origEntry : castsBySource.entrySet()) {
            final TClass source = origEntry.getKey();
            Map<TClass, TCast> filteredView = Maps.filterKeys(origEntry.getValue(), new Predicate<TClass>() {
                @Override
                public boolean apply(TClass target) {
                    return (source == target) || strongCasts.contains(new TCastIdentifier(source, target));
                }
            });
            assert ! filteredView.isEmpty() : "no strong casts (including self casts) found for " + source;
            result.put(source, new HashMap<TClass, TCast>(filteredView));
        }
        return result;
    }

    // private

    private static TCast cast(Map<TClass, Map<TClass, TCast>> castsBySource, TClass source, TClass target) {
        TCast result = null;
        Map<TClass,TCast> castsByTarget = castsBySource.get(source);
        if (castsByTarget != null)
            result = castsByTarget.get(target);
        return result;
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
            List<TClass> badPath = checker.getBadNodePath();
            // create a List<String> where everything is lowercase except for the first and last instances
            // of the offending node
            List<String> names = new ArrayList<String>(badPath.size());
            for (TClass tClass : badPath)
                names.add(tClass.toString().toLowerCase());
            String lastName = names.get(names.size() - 1);
            String lastNameUpper = lastName.toUpperCase();
            names.set(names.size() - 1, lastNameUpper);
            names.set(names.indexOf(lastName), lastNameUpper);
            throw new AkibanInternalException("non-DAG detected involving " + names);
        }
    }

    static void createDerivedCasts(Map<TClass,Map<TClass,TCast>> castsBySource, InstanceFinder finder) {
        for (TCastPath castPath : finder.find(TCastPath.class)) {
            List<? extends TClass> path = castPath.getPath();
            // We need this loop to protect against "jumps." For instance, let's say the cast path is
            // [ a, b, c, d, e ] and we have the following casts:
            //  "single step" casts: (a -> b), (b -> c), (c -> d), (d -> e)
            //  one "jump" cast: (a -> d),
            // The first pass of this loop will create a derived cast (a -> d -> e), but we wouldn't have created
            // (a -> c). This loop ensures that we will.
            // We work from both ends, shrinking iteratively from the beginning and recursively (within deriveCast)
            // from the end. A derived cast has to have at least three participants, so we can stop when we get
            // to a path whose size is less than 3.
            while (path.size() >= 3) {
                for (int i = path.size() - 1; i > 0; --i) {
                    deriveCast(castsBySource, path, i);
                }
                path = path.subList(1, path.size());
            }
        }
    }

    private static Map<TClass,Map<TClass,TCast>> createStrongCastsMap(Map<TClass, Map<TClass, TCast>> castsBySource,
                                                                      InstanceFinder finder)
    {
        Collection<? extends TStrongCasts> strongCastIds = finder.find(TStrongCasts.class);
        Set<TCastIdentifier> strongCasts = new HashSet<TCastIdentifier>(strongCastIds.size()); // rough guess
        for (TStrongCasts strongCastGenerator : strongCastIds) {
            for (TCastIdentifier castId : strongCastGenerator.get()) {
                TCast cast = cast(castsBySource, castId.getSource(), castId.getTarget());
                if (cast == null)
                    throw new AkibanInternalException("no cast defined for " + castId +", which is marked as strong");
                if (!strongCasts.add(castId)) {
                    logger.warn("multiple sources have listed cast {} as strong", castId);
                }
            }
        }
        return createStrongCastsMap(castsBySource, strongCasts);
    }

    /**
     * Add derived casts for any pair of TClasses (A, B) s.t. there is not a cast from A to B, but there are casts
     * from A to VARCHAR and from VARCHAR to B. This essentially uses VARCHAR as a base type. Not pretty, but effective.
     * Uses the instance variable #castsBySource for its input and output; it must be initialized with at least
     * the self-casts and declared casts.
     */
    private void deriveCastsFromVarchar() {
        final TClass COMMON = MString.VARCHAR;
        Set<TClass> tClasses = castsBySource.keySet();
        for (Map.Entry<TClass, Map<TClass, TCast>> entry : castsBySource.entrySet()) {
            TClass source = entry.getKey();
            Map<TClass, TCast> castsByTarget = entry.getValue();
            for (TClass target : tClasses) {
                if (target == source || castsByTarget.containsKey(target))
                    continue;
                TCast sourceToVarchar = cast(source, COMMON);
                if (sourceToVarchar == null)
                    continue;
                TCast varcharToTarget = cast(COMMON, target);
                if (varcharToTarget == null)
                    continue;
                TCast derived = new ChainedCast(sourceToVarchar, varcharToTarget);
                castsByTarget.put(target, derived);
            }
        }
    }

    private static TCast deriveCast(Map<TClass,Map<TClass,TCast>> castsBySource,
                                    List<? extends TClass> path, int targetIndex) {
        TClass source = path.get(0);
        TClass target = path.get(targetIndex);
        TCast alreadyThere = cast(castsBySource, source,  target);
        if (alreadyThere != null)
            return alreadyThere;
        int intermediateIndex = targetIndex - 1;
        TClass intermediateClass = path.get(intermediateIndex);
        TCast second = cast(castsBySource, intermediateClass, target);
        if (second == null)
            throw new AkibanInternalException("no explicit cast between " + intermediateClass + " and " + target
                    + " while creating cast path: " + path);
        TCast first = deriveCast(castsBySource, path, intermediateIndex);
        if (first == null)
            throw new AkibanInternalException("couldn't derive cast between " + source + " and " + intermediateClass
                    + " while creating cast path: " + path);
        TCast result = new ChainedCast(first, second);
        putCast(castsBySource, result, null);
        return result;
    }

    private static void putCast(Map<TClass, Map<TClass, TCast>> toMap, TCast cast, Set<TCastIdentifier> duplicates) {
        if (cast == null)
            return;
        TClass source = cast.sourceClass();
        TClass target = cast.targetClass();
        Map<TClass,TCast> castsByTarget = toMap.get(source);
        TCast old = castsByTarget.put(target, cast);
        if (old != null) {
            logger.error("CAST({} AS {}): {} replaced by {} ", new Object[]{
                    source, target,  old.getClass(), cast.getClass()
            });
            if (duplicates == null)
                throw new AkibanInternalException("multiple casts defined from " + source + " to " + target);
            duplicates.add(new TCastIdentifier(source, target));
        }
    }

    // nested classes

    private static class SelfCast implements TCast {

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
        public TInstance preferredTarget(TPreptimeValue source) {
            return source.instance();
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            if (source.isNull()) {
                target.putNull();
                return;
            }
            TInstance srcInst = context.inputTInstanceAt(0);
            TInstance dstInst = context.outputTInstance();
            tClass.selfCast(context, srcInst, source,  dstInst, target);
        }

        SelfCast(TClass tClass) {
            this.tClass = tClass;
        }

        private final TClass tClass;
    }

    static class ChainedCast implements TCast {

        @Override
        public Constantness constness() {
            Constantness firstConst = first.constness();
            return (firstConst == second.constness()) ? firstConst : Constantness.UNKNOWN;
        }

        @Override
        public TClass sourceClass() {
            return first.sourceClass();
        }

        @Override
        public TClass targetClass() {
            return second.targetClass();
        }

        @Override
        public TInstance preferredTarget(TPreptimeValue source) {
            TInstance intermediateTInstance = first.preferredTarget(source);
            PValueSource firstValue = source.value();
            TInstance result;
            if (firstValue != null) {
                PValue intermediateValue = new PValue(first.targetClass().underlyingType());
                TExecutionContext context = new TExecutionContext(
                        Collections.singletonList(source.instance()),
                        intermediateTInstance,
                        null // TODO is this null a problem?
                );
                try {
                    first.evaluate(context, firstValue, intermediateValue);
                    result = second.preferredTarget(new TPreptimeValue(intermediateTInstance, intermediateValue));
                } catch (Exception e) {
                    logger.error("while evaluating intermediate value for " + source + " in " + this, e);
                    result = second.preferredTarget(new TPreptimeValue(intermediateTInstance));
                }
            }
            else {
                result = second.preferredTarget(new TPreptimeValue(intermediateTInstance));
            }
            return result;
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            if (source.isNull()) {
                target.putNull();
                return;
            }
            PValue tmp = (PValue) context.exectimeObjectAt(TMP_PVALUE);
            if (tmp == null) {
                tmp = new PValue(first.targetClass().underlyingType());
                context.putExectimeObject(TMP_PVALUE, tmp);
            }
            // TODO cache
            TExecutionContext firstContext = context.deriveContext(
                    Collections.singletonList(context.inputTInstanceAt(0)),
                    intermediateType
            );
            TExecutionContext secondContext = context.deriveContext(
                    Collections.singletonList(intermediateType),
                    context.outputTInstance()
            );

            first.evaluate(firstContext, source, tmp);
            second.evaluate(secondContext, tmp, target);
        }

        private ChainedCast(TCast first, TCast second) {
            if (first.targetClass() != second.sourceClass()) {
                throw new IllegalArgumentException("can't chain casts: " + first + " and " + second);
            }
            this.first = first;
            this.second = second;
            this.intermediateType = first.targetClass().instance();
        }

        private final TCast first;
        private final TCast second;
        private final TInstance intermediateType;
        private static final int TMP_PVALUE = 0;
    }
}
