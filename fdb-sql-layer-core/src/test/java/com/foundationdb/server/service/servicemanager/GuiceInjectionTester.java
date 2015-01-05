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

package com.foundationdb.server.service.servicemanager;

import com.foundationdb.server.service.servicemanager.configuration.DefaultServiceConfigurationHandler;
import com.foundationdb.util.JUnitUtils;
import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class GuiceInjectionTester {

    public <I> GuiceInjectionTester bind(Class<I> anInterface, Class<? extends I> anImplementation) {
        configHandler.bind(anInterface.getName(), anImplementation.getName(), null);
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

    @SuppressWarnings("unchecked")
    public GuiceInjectionTester startAndStop(Class<?>... requiredClasses) {
        for (Class<?> requiredClass : requiredClasses) {
            configHandler.require(requiredClass.getName());
        }
        try {
            guicer = Guicer.forServices((Class<ServiceManagerBase>)serviceManagerInterfaceClass, serviceManager,
                                        configHandler.serviceBindings(true), configHandler.priorities(),
                                        Collections.<Module>emptyList());
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
        List<Class<?>> allClassesExpected = new ArrayList<>();
        allClassesExpected.add(aClass);
        Collections.addAll(allClassesExpected, itsDependencies);
        List<Class<?>> allClassesActual = new ArrayList<>();
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
        List<String> simpleNames = new ArrayList<>();
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

        private final List<Class<?>> stoppedClasses = new ArrayList<>();
    }

}
