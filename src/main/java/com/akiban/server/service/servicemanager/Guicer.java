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

import com.akiban.server.service.Service;
import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class Guicer {

    // Guicer interface

    public void startAllServices(ServiceLifecycleActions<?> withActions) {
        for (Class<?> directlyRequiredClass : directlyRequiredClasses) {
            injector.getInstance(directlyRequiredClass, withActions);
        }
    }

    public void stopAllServices(ServiceLifecycleActions<?> withActions) {
        injector.stopAllServices(withActions);
    }

    public <T> T get(Class<T> serviceClass, ServiceLifecycleActions<?> withActions) {
        return injector.getInstance(serviceClass, withActions);
    }

    // public class methods

    public static Guicer forServices(Collection<ServiceBinding> serviceBindings)
    throws ClassNotFoundException
    {
        return new Guicer(serviceBindings);
    }

    // private methods

    Guicer(Collection<ServiceBinding> serviceBindings)
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
        injector = new ServiceLifecycleInjector(Guice.createInjector(module));
    }

    // object state

    private final ServiceLifecycleInjector injector;
    private final List<Class<?>> directlyRequiredClasses;


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
                bind(unchecked).to(binding.serviceImplementationClass()).in(Singleton.class);
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
