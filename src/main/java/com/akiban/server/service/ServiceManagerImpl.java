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

import com.akiban.server.service.session.Session;

public final class ServiceManagerImpl
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

    private ServiceManagerImpl() {}

    /**
     * Gets the active ServiceManager; you can then use the returned instance to get any service you want.
     * @return the active ServiceManager
     * @deprecated for new code, please just use dependency injection
     */
    @Deprecated
    public static ServiceManager get() {
        return instance.get();
    }

    /**
     * Convenience for {@code ServiceManagerImpl.get().getSessionService().createSession()}
     * @return a new Session
     */
    public static Session newSession() {
        ServiceManager serviceManager = get();
        if (serviceManager == null) {
            throw new ServiceNotStartedException("ServiceManagerImpl.get() hasn't been given an instance");
        }
        return serviceManager.getSessionService().createSession();
    }
}
