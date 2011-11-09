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

package com.akiban.server.service;

import java.util.concurrent.atomic.AtomicReference;

import com.akiban.server.error.ServiceNotStartedException;
import com.akiban.server.service.servicemanager.DelegatingServiceManager;
import com.akiban.server.service.session.Session;

public final class ServiceManagerImpl extends DelegatingServiceManager
{
    private static final AtomicReference<ServiceManager> instance = new AtomicReference<ServiceManager>(null);

    public static void setServiceManager(ServiceManager newInstance) {
        if (newInstance == null) {
            instance.set(null);
        } else if (!instance.compareAndSet(null, newInstance)) {
            throw new RuntimeException(
                    "Tried to install a ServiceManager, but one was already set");
        }
    }

    public ServiceManagerImpl() {}

    /**
     * Gets the active ServiceManager; you can then use the returned instance to get any service you want.
     * @return the active ServiceManager
     * @deprecated for new code, please just use dependency injection
     */
    @Deprecated
    public static ServiceManager get() {
        return installed();
    }

    /**
     * Convenience for {@code ServiceManagerImpl.get().getSessionService().createSession()}
     * @return a new Session
     */
    public static Session newSession() {
        return installed().getSessionService().createSession();
    }

    // DelegatingServiceManager override


    @Override
    protected ServiceManager delegate() {
        return installed();
    }

    // for use by this class

    private static ServiceManager installed() {
        ServiceManager sm = instance.get();
        if (sm == null) {
            throw new ServiceNotStartedException("services haven't been started");
        }
        return sm;
    }
}
