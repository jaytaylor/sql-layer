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
import com.akiban.util.Exceptions;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

final class ServiceLifecycleInjector extends DelegatingInjector {

    @Override
    public <T> T getInstance(Key<T> key) {
        return startService(super.getInstance(key));
    }

    @Override
    public <T> T getInstance(Class<T> type) {
        return startService(super.getInstance(type));
    }

    public void stopAllServices() {
        try {
            stopServices(null);
        } catch (Exception e) {
            throw new RuntimeException(e); // TODO need better exception
        }
    }

    private <T> T startService(T instance) {
        if (instance instanceof Service) {
            try {
                Service<?> service = (Service<?>)instance;
                service.start();
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
        List<Service> stopServices = new ArrayList<Service>(servicesList.size());
        for (Service service : servicesList) {
            stopServices.add(service);
        }
        ListIterator<Service> reverseIter = stopServices
                .listIterator(stopServices.size());
        List<Throwable> exceptions = new ArrayList<Throwable>();
        if (initialCause != null) {
            exceptions.add(initialCause);
        }
        while (reverseIter.hasPrevious()) {
            try {
                Service service = reverseIter.previous();
                service.stop();
            } catch (Throwable t) {
                exceptions.add(t);
            }
        }
        return exceptions;
    }

    public ServiceLifecycleInjector(Injector delegate) {
        super(delegate);
        this.servicesList = new ArrayList<Service<?>>();
    }

    private final List<Service<?>> servicesList;
}
