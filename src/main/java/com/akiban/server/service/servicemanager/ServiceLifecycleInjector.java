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
import com.google.inject.ProvisionException;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

public final class ServiceLifecycleInjector {

    // ServiceLifecycleInjector interface

    public <T> T getInstance(Class<T> type, ServiceLifecycleActions<?> withActions) {
        return startService(injector.getInstance(type), withActions);
    }

    public boolean serviceIsStarted(Class<?> serviceClass) {
        synchronized (lock) {
            return services.contains(serviceClass);
        }
    }

    public void stopAllServices(ServiceLifecycleActions<?> withActions) {
        try {
            stopServices(withActions, null);
        } catch (Exception e) {
            throw new RuntimeException("while stopping services", e);
        }
    }

    public ServiceLifecycleInjector(Injector injector) {
        this.lock = new Object();
        this.injector = injector;
        // sync isn't technically required since services is final, but makes it clear that it's protected by the lock
        synchronized (lock) {
            this.services = new LinkedHashSet<Object>();
        }
    }

    // private methods

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

    private final Object lock;
    private final Set<Object> services;
    private final Injector injector;
}
