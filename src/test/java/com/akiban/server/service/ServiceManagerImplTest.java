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

import com.akiban.server.service.config.Property;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public final class ServiceManagerImplTest {
    @After
    public void unRegisterSMI() {
        ServiceManagerImpl.setServiceManager(null);
    }

    @Test
    public void loadCustomService() throws Exception {
        Property property = new Property(
                Property.parseKey(ServiceManagerImpl.CUSTOM_LOAD_SERVICE),
                MyService.class.getName()
        );
        ServiceManager sm = new ServiceManagerImpl(new UnitTestServiceFactory(false, Collections.singleton(property)));
        try {
            sm.startServices();
            MyService myService = sm.getServiceByClass(MyService.class);
            assertTrue("service not started", myService.isStarted());
        } finally {
            sm.stopServices();
        }
    }

    public static class MyService implements Service<MyService> {
        private final AtomicBoolean isStarted = new AtomicBoolean(false);
        @Override
        public MyService cast() {
            return this;
        }

        @Override
        public Class<MyService> castClass() {
            return MyService.class;
        }

        @Override
        public void start() throws Exception {
            if (!isStarted.compareAndSet(false, true)) {
                throw new ServiceStartupException("already started");
            }
        }

        @Override
        public void stop() throws Exception {
            if (!isStarted.compareAndSet(true, false)) {
                throw new Exception("already stopped");
            }
        }

        public boolean isStarted() {
            return isStarted.get();
        }
    }
}
