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

import com.akiban.util.Exceptions;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class ServiceLifecycleInjector extends DelegatingInjector {
// TODO use composition instead?

    // Injector interface

    @Override
    public <T> T getInstance(Key<T> key) {
        return startService(super.getInstance(key), GuicedServiceManager.STANDARD_SERVICE_ACTIONS);
    }

    @Override
    public <T> T getInstance(Class<T> type) {
        return startService(super.getInstance(type), GuicedServiceManager.STANDARD_SERVICE_ACTIONS);
    }

    // ServiceLifecycleInjector interface

    public <T> T getInstance(Class<T> type, ServiceLifecycleActions<?> withActions) {
        return startService(super.getInstance(type), withActions);
    }

    public void stopAllServices(ServiceLifecycleActions<?> withActions) {
        try {
            stopServices(withActions, null);
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO need better exception
        }
    }

    public <S> ServiceLifecycleInjector(Injector delegate) {
        super(delegate);
        this.servicesList = Collections.synchronizedSet(new LinkedHashSet<Object>());
    }

    public Collection<?> startedServices() {
        return Collections.unmodifiableCollection(servicesList);
    }

    // private methods

    private <T,S> T startService(T instance, ServiceLifecycleActions<S> withActions) {
        if (servicesList.contains(instance)) {
            return instance;
        }
        if (withActions == null) {
            servicesList.add(instance);
            return instance;
        }

        S service = withActions.castIfActionable(instance);
        if (service != null) {
            try {
                withActions.onStart(service);
                servicesList.add(service);
            } catch (Exception e) {
                try {
                    stopServices(withActions, e);
                } catch (Exception e1) {
                    e = e1;
                }
                throw new ProvisionException("While starting service " + instance.getClass(), e);
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
        ListIterator<?> reverseIter = new ArrayList<Object>(servicesList).listIterator(servicesList.size());
        servicesList.clear();
        List<Throwable> exceptions = new ArrayList<Throwable>();
        if (initialCause != null) {
            exceptions.add(initialCause);
        }
        while (reverseIter.hasPrevious()) {
            try {
                Object serviceObject = reverseIter.previous();
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
        return exceptions;
    }

    private final Set<Object> servicesList;
}
