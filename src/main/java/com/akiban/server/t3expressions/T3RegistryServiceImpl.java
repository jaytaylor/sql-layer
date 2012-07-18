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
import com.akiban.server.types3.TName;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.service.FunctionRegistry;
import com.akiban.server.types3.service.FunctionRegistryImpl;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.util.DagChecker;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        public String describeAll() {
            List<Object> all = new ArrayList<Object>();

            all.addAll(typesDescriptors());
            all.addAll(castsDescriptors());

            return toYaml(all);
        }

        private List<TypeDescriptor> typesDescriptors() {
            List<TypeDescriptor> result = new ArrayList<TypeDescriptor>(tClasses.size());
            for (TClass tClass : tClasses)
                result.add(new TypeDescriptor(tClass));
            Collections.sort(result);
            return result;
        }

        private List<CastDescriptor>  castsDescriptors() {
            List<CastDescriptor> result = new ArrayList<CastDescriptor>(castsBySource.size() * 5); // guess the size
            for (Map<TClass,TCast> castsByTarget : castsBySource.values()) {
                for (TCast tCast : castsByTarget.values()) {
                    result.add(new CastDescriptor(tCast));
                }
            }
            Collections.sort(result);
            return result;
        }

        private List<?>  buildScalars() {
            throw new UnsupportedOperationException();
        }

        private List<?>  buildAggregates() {
            throw new UnsupportedOperationException();
        }

        private String toYaml(Object obj) {
            return new Yaml().dump(obj);
        }
    }

    private static class TypeNameDescriptor implements Comparable<TypeNameDescriptor> {

        public String getBundle() {
            return tName.bundleId().name();
        }

        public String getName() {
            return tName.unqualifiedName();
        }

        @Override
        public int compareTo(TypeNameDescriptor o) {
            int cmp = getBundle().compareTo(o.getBundle());
            if (cmp == 0)
                cmp = getName().compareTo(o.getName());
            return cmp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypeNameDescriptor that = (TypeNameDescriptor) o;
            return tName.equals(that.tName);
        }

        @Override
        public int hashCode() {
            return tName.hashCode();
        }

        private TypeNameDescriptor(TName tName) {
            this.tName = tName;
        }

        private TName tName;
    }

    private static class TypeDescriptor implements Comparable<TypeDescriptor> {

        public TypeNameDescriptor getQualifiedName() {
            return name;
        }

        public int getInternalVersion() {
            return tClass.internalRepresentationVersion();
        }

        public int getSerializationVersion() {
            return tClass.serializationVersion();
        }

        public Integer getFixedSize() {
            return tClass.hasFixedSerializationSize() ? tClass.fixedSerializationSize() : null;
        }

        @Override
        public int compareTo(TypeDescriptor o) {
            return getQualifiedName().compareTo(o.getQualifiedName());
        }

        private TypeDescriptor(TClass tClass) {
            this.tClass = tClass;
            name = new TypeNameDescriptor(tClass.name());
        }

        private TClass tClass;
        private TypeNameDescriptor name;
    }

    private static class CastDescriptor implements Comparable<CastDescriptor> {

        public TypeNameDescriptor getSource() {
            return new TypeNameDescriptor(tCast.sourceClass().name());
        }

        public TypeNameDescriptor getTarget() {
            return new TypeNameDescriptor(tCast.targetClass().name());
        }

        public boolean isStrong() {
            return tCast.isAutomatic();
        }

        @Override
        public int compareTo(CastDescriptor o) {
            throw new UnsupportedOperationException(); // TODO
        }

        private CastDescriptor(TCast tCast) {
            this.tCast = tCast;
        }

        private TCast tCast;
    }
}
