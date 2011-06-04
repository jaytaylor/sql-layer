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
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Singleton;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class Guicer {

    // Guicer interface

    public Guicer(Collection<ServiceBinding> serviceBindings) throws ClassNotFoundException {
        directlyRequiredClasses = new ArrayList<Class<?>>();
        List<ResolvedServiceBinding> resolvedServiceBindings = new ArrayList<ResolvedServiceBinding>();

        for (ServiceBinding serviceBinding : serviceBindings) {
            ResolvedServiceBinding resolvedServiceBinding = new ResolvedServiceBinding(serviceBinding);
            resolvedServiceBindings.add(resolvedServiceBinding);
            if (serviceBinding.isDirectlyRequired()) {
                directlyRequiredClasses.add(resolvedServiceBinding.serviceInterfaceClass());
            }
        }
        injector = createInjector(resolvedServiceBindings);
    }

    public void startAllServices() {
        for (Class<?> directlyRequiredClass : directlyRequiredClasses) {
            injector.getInstance(directlyRequiredClass);
        }
    }

    public void stopAllServices() {
        injector.stopAllServices();
    }

    public <T> T get(Class<T> serviceClass) {
        return injector.getInstance(serviceClass);
    }

    // private methods

    private static ServiceLifecycleInjector createInjector(Collection<ResolvedServiceBinding> resolvedServiceBindings) {
        AbstractModule module = new ServiceBindingsModule(resolvedServiceBindings);
        return new ServiceLifecycleInjector(Guice.createInjector(module));
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
        }

        // object state
        private final Class<?> serviceInterfaceClass;
        private final Class<?> serviceImplementationClass;
    }

    private static final class ServiceBindingsModule extends AbstractModule {
        @Override
        protected void configure() {
            for (ResolvedServiceBinding biding : bindings) {
                bind(biding.serviceInterfaceClass()).to(biding.serviceImplementationClass()).in(Singleton.class);
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
