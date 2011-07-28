/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.servicemanager;

import com.akiban.server.service.servicemanager.configuration.DefaultServiceConfigurationHandler;
import com.akiban.util.JUnitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class GuiceInjectionTester<T> {

    public <I> GuiceInjectionTester<T> bind(Class<I> anInterface, Class<? extends I> anImplementation) {
        configHandler.bind(anInterface.getName(), anImplementation.getName());
        return this;
    }

    public GuiceInjectionTester<T> startAndStop(Class<?>... requiredClasses) {
        for (Class<?> requiredClass : requiredClasses) {
            configHandler.require(requiredClass.getName());
        }
        try {
            guicer = Guicer.forServices(configHandler.serviceBindings(), injectionHandler);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        for (Class<?> requiredClass : guicer.directlyRequiredClasses()) {
            guicer.get(requiredClass, shutdownHook);
        }
        startupOrder.addAll(injectionHandler.startedClasses());
        guicer.stopAllServices(shutdownHook);
        return this;
    }

    public GuiceInjectionTester<T> checkDependencies(Class<?> aClass, Class<?>... itsDependencies) {
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

    public GuiceInjectionTester<T> check(Class<?>... expectedClasses) {
        List<Class<?>> expectedList = Arrays.asList(expectedClasses);
        checkExactContents("startup", expectedList, startupOrder);
        checkExactContents("shutdown", expectedList, shutdownHook.stoppedClasses());
        return this;
    }

    private void checkExactContents(String whichList, List<Class<?>> expectedList, List<Class<?>> actualList) {
        assertEquals(whichList + " size", expectedList.size(), actualList.size());
        assertTrue(whichList + ": " + l(expectedList) + " != " + l(actualList), actualList.containsAll(expectedList));
    }

    private void checkSingleDependency(Class<?> aClass, Class<?> itsDependency) {
    // The class should appear after the dependency in startup, and before it for shutdown
        int aClassStartupOrder = findInList(aClass, startupOrder);
        int dependencyStartupOrder = findInList(itsDependency, startupOrder);
        assertTrue(
                String.format("%s started after %s: %s", n(itsDependency), n(aClass), l(startupOrder)),
                aClassStartupOrder > dependencyStartupOrder
        );

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

    public static <T> GuiceInjectionTester<T> forTarget(Class<T> targetClass) {
        return new GuiceInjectionTester<T>(targetClass);
    }

    private GuiceInjectionTester(Class<T> interestingClass) {
        injectionHandler = new ListingInjectionHandler<T>(interestingClass);
    }

    private final DefaultServiceConfigurationHandler configHandler = new DefaultServiceConfigurationHandler();
    private final ListingInjectionHandler<T> injectionHandler;
    private final ListOnShutdown shutdownHook = new ListOnShutdown();
    private final List<Class<?>> startupOrder = new ArrayList<Class<?>>();
    private Guicer guicer;

    // nested classes

    private static class ListingInjectionHandler<T> extends Guicer.InjectionHandler<T> {
        @Override
        protected void handle(T instance) {
            startedClasses.add(instance.getClass());
        }

        LinkedHashSet<Class<?>> startedClasses() {
            return startedClasses;
        }

        private ListingInjectionHandler(Class<T> targetClass) {
            super(targetClass);
        }

        private final LinkedHashSet<Class<?>> startedClasses = new LinkedHashSet<Class<?>>();
    }

    private static class ListOnShutdown implements Guicer.ServiceLifecycleActions<Object> {

        public List<Class<?>> stoppedClasses() {
            return stoppedClasses;
        }

        @Override
        public void onStart(Object service) throws Exception {
        }

        @Override
        public void onShutdown(Object service) throws Exception {
            stoppedClasses.add(service.getClass());
        }

        @Override
        public Object castIfActionable(Object object) {
            return object;
        }

        private final List<Class<?>> stoppedClasses = new ArrayList<Class<?>>();
    }

}
