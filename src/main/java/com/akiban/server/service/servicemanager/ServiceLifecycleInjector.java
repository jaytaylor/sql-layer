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
import java.util.List;
import java.util.ListIterator;

public final class ServiceLifecycleInjector<S> extends DelegatingInjector {

    // Injector interface

    @Override
    public <T> T getInstance(Key<T> key) {
        return startService(super.getInstance(key));
    }

    @Override
    public <T> T getInstance(Class<T> type) {
        return startService(super.getInstance(type));
    }

    // ServiceLifecycleInjector interface

    public void stopAllServices() {
        try {
            stopServices(null);
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO need better exception
        }
    }

    public ServiceLifecycleInjector(Injector delegate, ServiceLifecycleActions<S> actions) {
        super(delegate);
        this.servicesList = new ArrayList<S>();
        this.actions = actions;
    }

    // private methods

    private <T> T startService(T instance) {
        S service = actions.castIfActionable(instance);
        if (service != null) {
            try {
                actions.onStart(service);
                servicesList.add(service);
            } catch (Exception e) {
                try {
                    stopServices(e);
                } catch (Exception e1) {
                    e = e1;
                }
                throw new ProvisionException("While starting service " + instance.getClass(), e);
            }
        }
        return instance;
    }

    private void stopServices(Exception initialCause) throws Exception {
        List<Throwable> exceptions = tryStopServices(initialCause);
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

    private List<Throwable> tryStopServices(Exception initialCause) {
        ListIterator<S> reverseIter = servicesList.listIterator(servicesList.size());
        List<Throwable> exceptions = new ArrayList<Throwable>();
        if (initialCause != null) {
            exceptions.add(initialCause);
        }
        while (reverseIter.hasPrevious()) {
            try {
                S service = reverseIter.previous();
                reverseIter.remove();
                actions.onShutdown(service);
            } catch (Throwable t) {
                exceptions.add(t);
            }
        }
        return exceptions;
    }

    private final List<S> servicesList;
    private final ServiceLifecycleActions<S> actions;
}
