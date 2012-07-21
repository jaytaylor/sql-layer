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
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
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
import com.akiban.server.types3.service.FunctionRegistryImpl;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.util.DagChecker;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public final class T3RegistryServiceImpl implements T3RegistryService, Service<T3RegistryService>, JmxManageable {

    // T3RegistryService interface

    @Override
    public Collection<TValidatedOverload> getOverloads(String name) {
        return overloadsByName.get(name.toLowerCase());
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
        Map<TClass, TCast> castsFrom = strongCastsByTarget.get(tClass);
        return castsFrom.keySet();
    }

    @Override
    public Collection<? extends TAggregator> getAggregates(String name) {
        name = name.toLowerCase();
        Collection<? extends TAggregator> aggrs = aggregatorsByName.get(name);
        if (aggrs == null)
            throw new NoSuchFunctionException(name);
        return aggrs;
    }

    // Service interface

    @Override
    public T3RegistryService cast() {
        return this;
    }

    @Override
    public Class<T3RegistryService> castClass() {
        return T3RegistryService.class;
    }

    @Override
    public void start() {
        FunctionRegistry registry;
        try {
            registry = new FunctionRegistryImpl();
        } catch (Exception e) {
            logger.error("while creating registry", e);
            throw new ServiceStartupException("T3Registry");
        }
        start(registry);
    }

    @Override
    public void stop() {
        castsBySource = null;
        strongCastsByTarget = null;
        overloadsByName = null;
        aggregatorsByName = null;
        tClasses = null;
    }

    @Override
    public void crash() {
        stop();
    }

    // JmxManageable interface

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("T3Registry", new Bean(), T3RegistryMXBean.class);
    }

    // private methods

    private void start(FunctionRegistry finder) {
        tClasses = new HashSet<TClass>(finder.tclasses());
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

        strongCastsByTarget = createStrongCastsMap(castsBySource);
        checkDag(strongCastsByTarget);

        Multimap<String, TValidatedOverload> localOverloadsByName = ArrayListMultimap.create();
        for (TValidatedOverload overload : finder.overloads()) {
            localOverloadsByName.put(overload.overloadName().toLowerCase(), overload);
        }
        overloadsByName = Multimaps.unmodifiableMultimap(localOverloadsByName);

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

    // package-local; also used in testing
    static Map<TClass,Map<TClass,TCast>> createStrongCastsMap(Map<TClass, Map<TClass, TCast>> castsBySource) {
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

    // class state
    private static final Logger logger = LoggerFactory.getLogger(T3RegistryServiceImpl.class);

    // object state
    private Map<TClass,Map<TClass,TCast>> castsBySource;
    private Map<TClass,Map<TClass,TCast>> strongCastsByTarget;
    private Multimap<String, TValidatedOverload> overloadsByName;
    private Map<String,Collection<TAggregator>> aggregatorsByName;
    private Collection<? extends TClass> tClasses;

    // inner classes

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

    private class Bean implements T3RegistryMXBean {

        @Override
        public String describeTypes() {
            return toYaml(typesDescriptors());
        }

        @Override
        public String describeCasts() {
            return toYaml(castsDescriptors());
        }

        @Override
        public String describeScalars() {
            return toYaml(scalarDescriptors());
        }

        @Override
        public String describeAggregates() {
            return toYaml(aggregateDescriptors());
        }

        @Override
        public String describeAll() {
            Map<String,Object> all = new LinkedHashMap<String, Object>(5);

            all.put("types", typesDescriptors());
            all.put("casts", castsDescriptors());
            all.put("scalar_functions", scalarDescriptors());
            all.put("aggregate_functions", aggregateDescriptors());

            return toYaml(all);
        }

        private Object typesDescriptors() {
            List<Map<String,Comparable<?>>> result = new ArrayList<Map<String,Comparable<?>>>(tClasses.size());
            for (TClass tClass : tClasses) {
                Map<String,Comparable<?>> map = new LinkedHashMap<String, Comparable<?>>();
                buildTName("bundle", "name", tClass, map);
                map.put("category", tClass.name().category());
                map.put("internalVersion", tClass.internalRepresentationVersion());
                map.put("serializationVersion", tClass.serializationVersion());
                map.put("fixedSize", tClass.hasFixedSerializationSize() ? tClass.fixedSerializationSize() : null);
                result.add(map);
            }
            Collections.sort(result, new Comparator<Map<String, Comparable<?>>>() {
                @Override
                public int compare(Map<String, Comparable<?>> o1, Map<String, Comparable<?>> o2) {
                    return ComparisonChain.start()
                            .compare(o1.get("bundle"), o2.get("bundle"))
                            .compare(o1.get("category"), o2.get("category"))
                            .compare(o1.get("name"), o2.get("name"))
                            .result();
                }
            });
            return result;
        }

        private Object castsDescriptors() {
            // the starting size is just a guess
            List<Map<String,Comparable<?>>> result = new ArrayList<Map<String,Comparable<?>>>(castsBySource.size() * 5);
            for (Map<TClass,TCast> castsByTarget : castsBySource.values()) {
                for (TCast tCast : castsByTarget.values()) {
                    Map<String,Comparable<?>> map = new LinkedHashMap<String, Comparable<?>>();
                    buildTName("source_bundle", "source_type", tCast.sourceClass(), map);
                    buildTName("target_bundle", "target_type", tCast.targetClass(), map);
                    map.put("strong", tCast.isAutomatic());
                    result.add(map);
                }
            }
            Collections.sort(result, new Comparator<Map<String, Comparable<?>>>() {
                @Override
                public int compare(Map<String, Comparable<?>> o1, Map<String, Comparable<?>> o2) {
                    return ComparisonChain.start()
                            .compare(o1.get("source_bundle"), o2.get("source_bundle"))
                            .compare(o1.get("source_type"), o2.get("source_type"))
                            .compare(o1.get("target_bundle"), o2.get("target_bundle"))
                            .compare(o1.get("target_type"), o2.get("target_type"))
                            .result();
                }
            });
            return result;
        }

        private Object scalarDescriptors() {
            return describeOverloads(overloadsByName.asMap(), Functions.toStringFunction());
        }

        private Object aggregateDescriptors() {
            return describeOverloads(aggregatorsByName, new Function<TAggregator, TClass>() {
                @Override
                public TClass apply(TAggregator aggr) {
                    return aggr.getTypeClass();
                }
            });
        }

        private <T,S> Object describeOverloads(Map<String, Collection<T>> elems, Function<? super T, S> format) {
            Map<String,List<String>> result = new TreeMap<String, List<String>>();
            for (Map.Entry<String, ? extends Collection<T>> entry : elems.entrySet()) {
                Collection<T> overloads = entry.getValue();
                List<String> overloadDescriptions = new ArrayList<String>(overloads.size());
                for (T overload : overloads)
                    overloadDescriptions.add(String.valueOf(format.apply(overload)));
                Collections.sort(overloadDescriptions);
                result.put(entry.getKey(), overloadDescriptions);
            }
            return result;
        }

        private void buildTName(String bundleTag, String nameTag, TClass tClass, Map<String, Comparable<?>> out) {
            out.put(bundleTag, tClass.name().bundleId().name());
            out.put(nameTag, tClass.name().unqualifiedName());
        }

        private String toYaml(Object obj) {
            DumperOptions options = new DumperOptions();
            options.setAllowReadOnlyProperties(true);
            options.setDefaultFlowStyle(FlowStyle.BLOCK);
            options.setIndent(4);
            return new Yaml(options).dump(obj);
        }
    }
}
