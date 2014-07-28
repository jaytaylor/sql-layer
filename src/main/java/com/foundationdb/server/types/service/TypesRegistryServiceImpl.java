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

package com.foundationdb.server.types.service;

import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.model.aisb2.NewTableBuilder;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor;
import com.foundationdb.qp.memoryadapter.SimpleMemoryGroupScan;
import com.foundationdb.server.error.ServiceStartupException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.is.SchemaTablesService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverload;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.texpressions.TValidatedAggregator;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

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

public final class TypesRegistryServiceImpl 
    implements TypesRegistryService, Service, JmxManageable {

    private static TypesRegistryService INSTANCE = null;
    public static TypesRegistryService createRegistryService() {
        if (INSTANCE == null) {
            TypesRegistryServiceImpl registryService = new TypesRegistryServiceImpl();
            registryService.start();
            INSTANCE = registryService;
        }
        return INSTANCE;
    }
    public TypesRegistryServiceImpl() {
    }

    // TypesRegistryService interface

    @Override
    public TypesRegistry getTypesRegistry() {
        return typesRegistry;
    }

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

    @Override
    public FunctionKind getFunctionKind(String name) {
        if (scalarsResolver.isDefined(name))
            return FunctionKind.SCALAR;
        else if (aggregatesResolver.isDefined(name))
            return FunctionKind.AGGREGATE;
        else
            return null;
    }

    // Service interface

    @Override
    public void start() {
        InstanceFinder registry;
        try {
            registry = new ReflectiveInstanceFinder();
        } catch (Exception e) {
            logger.error("while creating registry", e);
            throw new ServiceStartupException("TypesRegistry");
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
        return new JmxObjectInfo("TypesRegistry", new Bean(), TypesRegistryMXBean.class);
    }

    // private methods

    void start(InstanceFinder finder) {
        tClasses = new HashSet<>(finder.find(TClass.class));

        typesRegistry = new TypesRegistry(tClasses);

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
        scalarsResolver = new OverloadResolver<>(scalarsRegistry, castsResolver);

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
        aggregatesResolver = new OverloadResolver<>(aggreatorsRegistry, castsResolver);

        keyComparableRegistry = new KeyComparableRegistry(finder);
    }

    // class state
    private static final Logger logger = LoggerFactory.getLogger(TypesRegistryServiceImpl.class);

    // object state

    private volatile TypesRegistry typesRegistry;
    private volatile TCastResolver castsResolver;
    private volatile ResolvablesRegistry<TValidatedAggregator> aggreatorsRegistry;
    private volatile OverloadResolver<TValidatedAggregator> aggregatesResolver;
    private volatile ResolvablesRegistry<TValidatedScalar> scalarsRegistry;
    private volatile OverloadResolver<TValidatedScalar> scalarsResolver;
    private volatile KeyComparableRegistry keyComparableRegistry;

    private volatile Collection<? extends TClass> tClasses;

    // inner classes
    
    public class TypesRegistrySchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service {

        @Inject
        public TypesRegistrySchemaTablesServiceImpl(SchemaManager schemaManager) {
            super(schemaManager);
        }

        @Override
        public void start() {
            TableName overloadsName = TableName.create(TableName.INFORMATION_SCHEMA, "ak_overloads");
            OverloadsTableFactory overloadsFactory = new OverloadsTableFactory(overloadsName);
            attach(overloadsFactory.table(schemaManager.getTypesTranslator()),  overloadsFactory);
            
            TableName castsName = TableName.create(TableName.INFORMATION_SCHEMA, "ak_casts");
            CastsTableFactory castsFactory = new CastsTableFactory(castsName);
            attach (castsFactory.table(schemaManager.getTypesTranslator()), castsFactory);
        }

        @Override
        public void stop() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void crash() {
            // TODO Auto-generated method stub
            
        }
        
    }

    private class Bean implements TypesRegistryMXBean {

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
            Map<String,Object> all = new LinkedHashMap<>(5);

            all.put("types", typesDescriptors());
            all.put("casts", castsDescriptors());
            all.put("scalar_functions", describeOverloads(scalarsRegistry));
            all.put("aggregate_functions", describeOverloads(aggreatorsRegistry));

            return toYaml(all);
        }

        private Object typesDescriptors() {
            List<Map<String,Comparable<?>>> result = new ArrayList<>(tClasses.size());
            for (TClass tClass : tClasses) {
                Map<String,Comparable<?>> map = new LinkedHashMap<>();
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
            List<Map<String,Comparable<?>>> result = new ArrayList<>(castsBySource.size() * 5);
            for (Map<TClass,TCast> castsByTarget : castsBySource) {
                for (TCast tCast : castsByTarget.values()) {
                    Map<String,Comparable<?>> map = new LinkedHashMap<>();
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
            Map<String,Map<String,String>> result = new TreeMap<>();
            for (Map.Entry<String, ? extends Collection<T>> entry : elems.entrySet()) {
                Collection<T> overloads = entry.getValue();
                Map<String,String> overloadDescriptions = new TreeMap<>();
                int idSuffix = 1;
                boolean allSamePriorities = allSamePriorities(overloads);
                if (!allSamePriorities) {
                    List<T> asList = new ArrayList<>(overloads);
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
        };
    }

    private static int lowest(int[] ints) {
        int result = ints[0];
        for (int i = 1; i < ints.length; ++i)
            result = Math.min(result, ints[i]);
        return result;
    }

    private abstract class MemTableBase extends BasicFactoryBase {

        protected abstract void buildTable(NewTableBuilder builder);

        public Table table(TypesTranslator typesTranslator) {
            NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator);
            buildTable(builder.table(getName()));
            return builder.ais(false).getTable(getName());
        }

        protected MemTableBase(TableName tableName) {
            super(tableName);
        }
    }

    private class CastsTableFactory extends MemTableBase {

        @Override
        protected void buildTable(NewTableBuilder builder) {
            builder.colString("source_bundle", 64, false)
                    .colString("source_type", 64, false)
                    .colString("target_bundle", 64, false)
                    .colString("target_type", 64, false)
                    .colString("is_strong", 3, false)
                    .colString("is_derived", 3, false);
        }

        @Override
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter) {
            Collection<Map<TClass, TCast>> castsBySource = castsResolver.castsBySource();
            Collection<TCast> castsCollections = new ArrayList<>(castsBySource.size());
            for (Map<?, TCast> castMap : castsBySource) {
                castsCollections.addAll(castMap.values());
            }
            return new SimpleMemoryGroupScan<TCast>(adapter, getName(), castsCollections.iterator()) {
                @Override
                protected Object[] createRow(TCast data, int hiddenPk) {
                    return new Object[] {
                            data.sourceClass().name().bundleId().name(),
                            data.sourceClass().name().unqualifiedName(),
                            data.targetClass().name().bundleId().name(),
                            data.targetClass().name().unqualifiedName(),
                            boolResult(castsResolver.isStrong(data)),
                            boolResult(data instanceof TCastsRegistry.ChainedCast),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            for (Map<?,?> castsBySource : castsResolver.castsBySource()) {
                count += castsBySource.size();
            }
            return count;
        }

        private CastsTableFactory(TableName tableName) {
            super(tableName);
        }
    }

    private class OverloadsTableFactory extends MemTableBase {

        @Override
        protected void buildTable(NewTableBuilder builder) {
            builder.colString("name", 128, false)
                   .colBigInt("priority_order", false)
                   .colString("inputs", 256, false)
                   .colString("output", 256, false)
                   .colString("internal_impl", 256, false);
        }

        @Override
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter) {
            Iterator<? extends TValidatedOverload> allOverloads = Iterators.concat(
                    scalarsRegistry.iterator(),
                    aggreatorsRegistry.iterator());
            return new SimpleMemoryGroupScan<TValidatedOverload>(adapter, getName(), allOverloads) {
                @Override
                protected Object[] createRow(TValidatedOverload data, int hiddenPk) {
                    return new Object[] {
                            data.displayName().toLowerCase(),
                            lowest(data.getPriorities()),
                            data.describeInputs(),
                            data.resultStrategy().toString(true),
                            data.id(),
                            hiddenPk
                    };
                }
            };
        }

        @Override
        public long rowCount(Session session) {
            return countOverloads(aggreatorsRegistry) + countOverloads(scalarsRegistry);
        }

        private long countOverloads(ResolvablesRegistry<?> registry) {
            long count = 0;
            for (ScalarsGroup<?> group : registry.allScalarsGroups()) {
                count += group.getOverloads().size();
            }
            return count;
        }

        public OverloadsTableFactory(TableName tableName) {
            super(tableName);
        }
    }

}
