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

import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.util.Exceptions;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import com.google.inject.Scopes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class Guicer {

    // Guicer interface

    public void startRequiredServices(ServiceLifecycleActions<?> withActions) {
        for (Class<?> directlyRequiredClass : directlyRequiredClasses) {
            get(directlyRequiredClass, withActions);
        }
    }

    public void stopAllServices(ServiceLifecycleActions<?> withActions) {
        try {
            stopServices(withActions, null);
        } catch (Exception e) {
            throw new RuntimeException("while stopping services", e);
        }
    }

    public <T> T get(Class<T> serviceClass, ServiceLifecycleActions<?> withActions) {
        return startService(_injector.getInstance(serviceClass), withActions);
    }

    public boolean serviceIsStarted(Class<?> serviceClass) {
        synchronized (lock) {
            return services.contains(serviceClass);
        }
    }

    public boolean isRequired(Class<?> interfaceClass) {
        return directlyRequiredClasses.contains(interfaceClass);
    }


    // public class methods

    public static Guicer forServices(Collection<ServiceBinding> serviceBindings)
    throws ClassNotFoundException
    {
        return new Guicer(serviceBindings);
    }

    // private methods

    private Guicer(Collection<ServiceBinding> serviceBindings)
    throws ClassNotFoundException
    {
        directlyRequiredClasses = new ArrayList<Class<?>>();
        List<ResolvedServiceBinding> resolvedServiceBindings = new ArrayList<ResolvedServiceBinding>();

        for (ServiceBinding serviceBinding : serviceBindings) {
            ResolvedServiceBinding resolvedServiceBinding = new ResolvedServiceBinding(serviceBinding);
            resolvedServiceBindings.add(resolvedServiceBinding);
            if (serviceBinding.isDirectlyRequired()) {
                directlyRequiredClasses.add(resolvedServiceBinding.serviceInterfaceClass());
            }
        }

        AbstractModule module = new ServiceBindingsModule(resolvedServiceBindings);
        _injector = Guice.createInjector(module);

        // sync isn't technically required since services is final, but makes it clear that it's protected by the lock
        lock = new Object();
        synchronized (lock) {
            this.services = new LinkedHashSet<Object>();
        }
    }

    private <T,S> T startService(T instance, ServiceLifecycleActions<S> withActions) {
        synchronized (lock) {
            if (services.contains(instance)) {
                return instance;
            }
            if (withActions == null) {
                services.add(instance);
                return instance;
            }

            S service = withActions.castIfActionable(instance);
            if (service != null) {
                try {
                    withActions.onStart(service);
                    services.add(service);
                } catch (Exception e) {
                    try {
                        stopServices(withActions, e);
                    } catch (Exception e1) {
                        e = e1;
                    }
                    throw new ProvisionException("While starting service " + instance.getClass(), e);
                }
            }
        }
        return instance;
    }

    private void stopServices(ServiceLifecycleActions<?> withActions, Exception initialCause) throws Exception {
        List<Throwable> exceptions = tryStopServices(withActions, initialCause);
        if (!exceptions.isEmpty()) {
            if (exceptions.size() == 1) {
                throw Exceptions.throwAlways(exceptions.get(0));
            }
            for (Throwable t : exceptions) {
                t.printStackTrace();
            }
            throw new Exception("Failure(s) while shutting down services: " + exceptions, exceptions.get(0));
        }
    }

    private <S> List<Throwable> tryStopServices(ServiceLifecycleActions<S> withActions, Exception initialCause) {
        ListIterator<?> reverseIter;
        synchronized (lock) {
            reverseIter = new ArrayList<Object>(services).listIterator(services.size());
        }
        List<Throwable> exceptions = new ArrayList<Throwable>();
        if (initialCause != null) {
            exceptions.add(initialCause);
        }
        while (reverseIter.hasPrevious()) {
            try {
                Object serviceObject = reverseIter.previous();
                synchronized (lock) {
                    services.remove(serviceObject);
                }
                if (withActions != null) {
                    S service = withActions.castIfActionable(serviceObject);
                    if (service != null) {
                        withActions.onShutdown(service);
                    }
                }
            } catch (Throwable t) {
                exceptions.add(t);
            }
        }
        // TODO because our dependency graph is created via Service.start() invocations, if service A uses service B
        // in stop() but not start(), and service B has already been shut down, service B will be resurrected. Yuck.
        // I don't know of a good way around this, other than by formalizing our dependency graph via constructor
        // params (and thus removing ServiceManagerImpl.get() ). Until this is resolved, simplest is to just shrug
        // our shoulders and not check
//        synchronized (lock) {
//            assert services.isEmpty() : services;
//        }
        return exceptions;
    }

    // object state

    private final List<Class<?>> directlyRequiredClasses;
    private final Object lock;
    private final Set<Object> services;
    private final Injector _injector;

    // nested classes

    private static final class ResolvedServiceBinding {

        // ResolvedServiceBinding interface

        public Class<?> serviceInterfaceClass() {
            return serviceInterfaceClass;
        }

        public Class<?> serviceImplementationClass() {
            return serviceImplementationClass;
        }

        public ResolvedServiceBinding(ServiceBinding serviceBinding) throws ClassNotFoundException {
            this.serviceInterfaceClass = Class.forName(serviceBinding.getInterfaceName());
            this.serviceImplementationClass = Class.forName(serviceBinding.getImplementingClassName());
            if (!this.serviceInterfaceClass.isAssignableFrom(this.serviceImplementationClass)) {
                throw new IllegalArgumentException(this.serviceInterfaceClass + " is not assignable from "
                        + this.serviceImplementationClass);
            }
        }

        // object state
        private final Class<?> serviceInterfaceClass;
        private final Class<?> serviceImplementationClass;
    }

    private static final class ServiceBindingsModule extends AbstractModule {
        @Override
        // we use unchecked, raw Class, relying on the invariant established by ResolvedServiceBinding's ctor
        @SuppressWarnings("unchecked")
        protected void configure() {
            for (ResolvedServiceBinding binding : bindings) {
                Class unchecked = binding.serviceInterfaceClass();
                bind(unchecked).to(binding.serviceImplementationClass()).in(Scopes.SINGLETON);
            }
        }

        // ServiceBindingsModule interface

        private ServiceBindingsModule(Collection<ResolvedServiceBinding> bindings) {
            this.bindings = bindings;
        }

        // object state

        private final Collection<ResolvedServiceBinding> bindings;
    }
}
