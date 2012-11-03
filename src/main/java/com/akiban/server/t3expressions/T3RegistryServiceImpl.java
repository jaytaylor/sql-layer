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

import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TKeyComparable;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.service.InstanceFinder;
import com.akiban.server.types3.service.ReflectiveInstanceFinder;
import com.akiban.server.types3.texpressions.TValidatedAggregator;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class T3RegistryServiceImpl implements T3RegistryService, Service, JmxManageable {

    public static TCastResolver createTCastResolver() {
        T3RegistryServiceImpl registryService = new T3RegistryServiceImpl();
        registryService.start();
        return registryService.getCastsResolver();
    }

    // T3RegistryService interface

    @Override
    public OverloadResolver<TValidatedScalar> getScalarsResolver() {
        return scalarsResolver;
    }

    @Override
    public OverloadResolver<TValidatedAggregator> getAggregatesResolver() {
        return aggregatesResolver;
    }

    @Override
    public TCastResolver getCastsResolver() {
        return castsResolver;
    }

    @Override
    public TKeyComparable getKeyComparable(TClass left, TClass right) {
        if (left == null || right == null)
            return null;
        return keyComparableRegistry.getClass(left, right);
    }

    // Service interface

    @Override
    public void start() {
        InstanceFinder registry;
        try {
            registry = new ReflectiveInstanceFinder();
        } catch (Exception e) {
            logger.error("while creating registry", e);
            throw new ServiceStartupException("T3Registry");
        }
        start(registry);
    }

    @Override
    public void stop() {
        castsResolver = null;
        scalarsRegistry = null;
        aggreatorsRegistry = null;
        tClasses = null;
        keyComparableRegistry = null;
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

    void start(InstanceFinder finder) {
        tClasses = new HashSet<TClass>(finder.find(TClass.class));

        TCastsRegistry castsRegistry = new TCastsRegistry(tClasses, finder);
        castsResolver = new TCastResolver(castsRegistry);

        scalarsRegistry = ResolvablesRegistry.create(
                finder,
                castsResolver,
                TScalar.class,
                new Function<TScalar, TValidatedScalar>() {
                    @Override
                    public TValidatedScalar apply(TScalar input) {
                        return new TValidatedScalar(input);
                    }
                }, new Function<TValidatedScalar, TValidatedScalar>() {
                    @Override
                    public TValidatedScalar apply(TValidatedScalar input) {
                        return input.createCommuted();
                    }
                }
        );
        scalarsResolver = new OverloadResolver<TValidatedScalar>(scalarsRegistry, castsResolver);

        aggreatorsRegistry = ResolvablesRegistry.create(
                finder,
                castsResolver,
                TAggregator.class,
                new Function<TAggregator, TValidatedAggregator>() {
                    @Override
                    public TValidatedAggregator apply(TAggregator input) {
                        return new TValidatedAggregator(input);
                    }
                },
                null
        );
        aggregatesResolver = new OverloadResolver<TValidatedAggregator>(aggreatorsRegistry, castsResolver);

        keyComparableRegistry = new KeyComparableRegistry(finder);
    }

    // class state
    private static final Logger logger = LoggerFactory.getLogger(T3RegistryServiceImpl.class);

    // object state

    private volatile TCastResolver castsResolver;
    private volatile ResolvablesRegistry<TValidatedAggregator> aggreatorsRegistry;
    private volatile OverloadResolver<TValidatedAggregator> aggregatesResolver;
    private volatile ResolvablesRegistry<TValidatedScalar> scalarsRegistry;
    private volatile OverloadResolver<TValidatedScalar> scalarsResolver;
    private volatile KeyComparableRegistry keyComparableRegistry;

    private volatile Collection<? extends TClass> tClasses;

    // inner classes

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
            return toYaml(describeOverloads(scalarsRegistry));
        }

        @Override
        public String describeAggregates() {
            return toYaml(describeOverloads(aggreatorsRegistry));
        }

        @Override
        public String describeAll() {
            Map<String,Object> all = new LinkedHashMap<String, Object>(5);

            all.put("types", typesDescriptors());
            all.put("casts", castsDescriptors());
            all.put("scalar_functions", describeOverloads(scalarsRegistry));
            all.put("aggregate_functions", describeOverloads(aggreatorsRegistry));

            return toYaml(all);
        }

        @Override
        public boolean isNewtypesOn() {
            return Types3Switch.ON;
        }

        private Object typesDescriptors() {
            List<Map<String,Comparable<?>>> result = new ArrayList<Map<String,Comparable<?>>>(tClasses.size());
            for (TClass tClass : tClasses) {
                Map<String,Comparable<?>> map = new LinkedHashMap<String, Comparable<?>>();
                buildTName("bundle", "name", tClass, map);
                map.put("category", tClass.name().categoryName());
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
            Collection<Map<TClass, TCast>> castsBySource = castsResolver.castsBySource();
            List<Map<String,Comparable<?>>> result = new ArrayList<Map<String,Comparable<?>>>(castsBySource.size() * 5);
            for (Map<TClass,TCast> castsByTarget : castsBySource) {
                for (TCast tCast : castsByTarget.values()) {
                    Map<String,Comparable<?>> map = new LinkedHashMap<String, Comparable<?>>();
                    buildTName("source_bundle", "source_type", tCast.sourceClass(), map);
                    buildTName("target_bundle", "target_type", tCast.targetClass(), map);
                    map.put("strong", castsResolver.isStrong(tCast));
                    map.put("isDerived", tCast instanceof TCastsRegistry.ChainedCast);
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

        private <V extends TValidatedOverload> Object describeOverloads(ResolvablesRegistry<V> registry) {
            Multimap<String, TOverload> flattenedOverloads = HashMultimap.create();
            for (Map.Entry<String, ScalarsGroup<V>> entry : registry.entriesByName()) {
                String overloadName = entry.getKey();
                ScalarsGroup<V> scalarsGroup = entry.getValue();
                flattenedOverloads.putAll(overloadName, scalarsGroup.getOverloads());
            }
            return describeOverloads(flattenedOverloads.asMap(), Functions.toStringFunction());
        }

        private <T extends TOverload,S> Object describeOverloads(
                Map<String, Collection<T>> elems, Function<? super T, S> format)
        {
            Map<String,Map<String,String>> result = new TreeMap<String, Map<String,String>>();
            for (Map.Entry<String, ? extends Collection<T>> entry : elems.entrySet()) {
                Collection<T> overloads = entry.getValue();
                Map<String,String> overloadDescriptions = new TreeMap<String, String>();
                int idSuffix = 1;
                boolean allSamePriorities = allSamePriorities(overloads);
                if (!allSamePriorities) {
                    List<T> asList = new ArrayList<T>(overloads);
                    Collections.sort(asList, compareByPriorities);
                    overloads = asList;
                }
                for (T overload : overloads) {
                    final String overloadId = overload.id();
                    final String origDescription = String.valueOf(format.apply(overload));
                    String overloadDescription = origDescription;

                    // We don't care about efficiency in this loop, so let's keep the code simple
                    while (overloadDescriptions.containsKey(overloadDescription)) {
                        overloadDescription = origDescription + " [" + Integer.toString(idSuffix++) + ']';
                    }
                    if (!allSamePriorities)
                        overloadDescription = "priority " + Arrays.toString(overload.getPriorities()) + ' '
                                + origDescription;
                    overloadDescriptions.put(overloadDescription, overloadId);
                }
                result.put(entry.getKey(), overloadDescriptions);
            }
            return result;
        }

        private <T extends TOverload> boolean allSamePriorities(Collection<T> overloads) {
            Iterator<T> iter = overloads.iterator();
            int[] firstPriorities = iter.next().getPriorities();
            while (iter.hasNext()) {
                int[] priorities = iter.next().getPriorities();
                if (!Arrays.equals(firstPriorities, priorities))
                    return false;
            }
            return true;
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

        Comparator<TOverload> compareByPriorities = new Comparator<TOverload>() {
            @Override
            public int compare(TOverload o1, TOverload o2) {
                int[] o1Priorities = o1.getPriorities();
                int[] o2Priorities = o2.getPriorities();
                return lowest(o1Priorities) - lowest(o2Priorities);
            }

            private int lowest(int[] ints) {
                int result = ints[0];
                for (int i = 1; i < ints.length; ++i)
                    result = Math.min(result, ints[i]);
                return result;
            }
        };
    }

}
