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

package com.akiban.server.service.servicemanager;

import com.akiban.server.service.servicemanager.configuration.DefaultServiceConfigurationHandler;
import com.akiban.util.JUnitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class GuiceInjectionTester {

    public <I> GuiceInjectionTester bind(Class<I> anInterface, Class<? extends I> anImplementation) {
        configHandler.bind(anInterface.getName(), anImplementation.getName());
        return this;
    }

    public <I extends ServiceManagerBase> GuiceInjectionTester manager(Class<I> serviceManagerInterfaceClass,
                                        I serviceManager) {
        this.serviceManagerInterfaceClass = serviceManagerInterfaceClass;
        this.serviceManager = serviceManager;
        return this;
    }

    public <I> GuiceInjectionTester prioritize(Class<I> anInterface) {
        configHandler.prioritize(anInterface.getName());
        return this;
    }

    public GuiceInjectionTester startAndStop(Class<?>... requiredClasses) {
        for (Class<?> requiredClass : requiredClasses) {
            configHandler.require(requiredClass.getName());
        }
        try {
            guicer = Guicer.forServices((Class)serviceManagerInterfaceClass, serviceManager,
                                        configHandler.serviceBindings(), configHandler.priorities());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (Class<?> requiredClass : guicer.directlyRequiredClasses()) {
            guicer.get(requiredClass, shutdownHook);
        }
        guicer.stopAllServices(shutdownHook);
        return this;
    }

    public GuiceInjectionTester checkDependencies(Class<?> aClass, Class<?>... itsDependencies) {
        for (Class<?> dependency : itsDependencies) {
            checkSingleDependency(aClass, dependency);
        }

        // alternate method
        List<Class<?>> allClassesExpected = new ArrayList<Class<?>>();
        allClassesExpected.add(aClass);
        Collections.addAll(allClassesExpected, itsDependencies);
        List<Class<?>> allClassesActual = new ArrayList<Class<?>>();
        for (Object instance : guicer.dependenciesFor(aClass)) {
            allClassesActual.add(instance.getClass());
        }
        JUnitUtils.equalCollections("for " + aClass, allClassesExpected, allClassesActual);
        return this;
    }

    public GuiceInjectionTester check(Class<?>... expectedClasses) {
        List<Class<?>> expectedList = Arrays.asList(expectedClasses);
        checkExactContents("shutdown", expectedList, shutdownHook.stoppedClasses());
        return this;
    }

    private void checkExactContents(String whichList, List<Class<?>> expectedList, List<Class<?>> actualList) {
        if (expectedList.size() != actualList.size()) {
            JUnitUtils.equalCollections(whichList + " lists not of same size", expectedList, actualList);
        }
        assertEquals(whichList + " size", expectedList.size(), actualList.size());
        assertTrue(whichList + ": " + l(expectedList) + " != " + l(actualList), actualList.containsAll(expectedList));
    }

    private void checkSingleDependency(Class<?> aClass, Class<?> itsDependency) {
    // The class should appear after the dependency in startup, and before it for shutdown
        List<Class<?>> shutdownOrder = shutdownHook.stoppedClasses();
        int aClassShutdownOrder = findInList(aClass, shutdownOrder);
        int dependencyShutdownOrder = findInList(itsDependency, shutdownOrder);
        assertTrue(
                String.format("%s stopped before %s: %s", n(itsDependency), n(aClass), l(shutdownOrder)),
                aClassShutdownOrder < dependencyShutdownOrder
        );
    }

    private static String n(Class<?> aClass) {
        return aClass.getSimpleName();
    }

    private List<String> l(List<Class<?>> list) {
        List<String> simpleNames = new ArrayList<String>();
        for (Class<?> aClass : list) {
            simpleNames.add(n(aClass));
        }
        return simpleNames;
    }

    private int findInList(Class<?> aClass, List<Class<?>> list) {
        int index = list.indexOf(aClass);
        assertTrue(aClass + " not in list " + list, index >= 0);
        return index;
    }

    private final DefaultServiceConfigurationHandler configHandler = new DefaultServiceConfigurationHandler();
    private final ListOnShutdown shutdownHook = new ListOnShutdown();
    private Class<? extends ServiceManagerBase> serviceManagerInterfaceClass;
    private ServiceManagerBase serviceManager;
    private Guicer guicer;

    // nested classes

    private static class ListOnShutdown implements Guicer.ServiceLifecycleActions<Object> {

        public List<Class<?>> stoppedClasses() {
            return stoppedClasses;
        }

        @Override
        public void onStart(Object service) {
        }

        @Override
        public void onShutdown(Object service) {
            stoppedClasses.add(service.getClass());
        }

        @Override
        public Object castIfActionable(Object object) {
            return object;
        }

        private final List<Class<?>> stoppedClasses = new ArrayList<Class<?>>();
    }

}
