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
import com.akiban.server.service.jmx.JmxRegistryService;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

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

    @Test
    public void shutdownOnFailedStartup() throws Exception {
        CrashOnStartupService service = new CrashOnStartupService(
                new UnitTestServiceFactory(false, Collections.<Property>emptySet())
        );

        PrintStream devSlashNull = new PrintStream(new ByteArrayOutputStream());
        final PrintStream oldErr = System.err;
        ServiceStartupException exception = null;
        try {
            System.setErr(devSlashNull);
            service.startServices();
        } catch (ServiceStartupException e) {
            exception = e;
        } finally {
            System.setErr(oldErr);
        }


        List<String> expected = Arrays.asList(
                "starting FirstService",
                "starting SecondService",

                "about to crash on startup",
                
                "stopping SecondService",
                "SecondService about to crash on shutdown",
                "stopping FirstService",
                "FirstService about to crash on shutdown"
        );
        assertEquals("messages", expected, service.getMessages());
        assertNotNull("excpected ServiceStartupException", exception);
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
        
        @Override
        public void crash() throws Exception {
            stop();
        }


        public boolean isStarted() {
            return isStarted.get();
        }
    }

    private static class CrashOnStartupService extends ServiceManagerImpl {

        private final List<String> messages = new ArrayList<String>();

        public CrashOnStartupService(ServiceFactory factory) {
            super(factory);
        }

        @Override
        void startAndPutServices(JmxRegistryService jmxRegistry) throws Exception {
            startAndPut(new FirstService(messages), jmxRegistry);
            startAndPut(new SecondService(messages), jmxRegistry);
            messages.add("about to crash on startup");
            throw new CrashOnStartupException();
        }

        public List<String> getMessages() {
            return messages;
        }
    }

    private static class CrashOnStartupException extends Exception {
    }

    public static abstract class TalkingService<T> implements Service<T> {

        private final List<String> messages;

        private TalkingService(List<String> messages) {
            this.messages = messages;
        }

        protected final void say(String message) {
            messages.add(message);
        }

        @Override
        public void start() throws Exception {
            say("starting " + this.getClass().getSimpleName());
        }

        @Override
        public void stop() throws Exception {
            say("stopping " + this.getClass().getSimpleName());
            say(this.getClass().getSimpleName() + " about to crash on shutdown");
            throw new UnsupportedOperationException();
        }

        @Override
        public void crash() throws Exception {
            say("crashing " + this.getClass().getSimpleName());
        }
    }

    public static class FirstService extends TalkingService<FirstService> {

        public FirstService(List<String> messages) {
            super(messages);
        }

        @Override
        public Class<FirstService> castClass() {
            return FirstService.class;
        }

        @Override
        public FirstService cast() {
            return this;
        }
    }

    public static class SecondService extends TalkingService<SecondService> {

        public SecondService(List<String> messages) {
            super(messages);
        }

        @Override
        public Class<SecondService> castClass() {
            return SecondService.class;
        }

        @Override
        public SecondService cast() {
            return this;
        }
    }
}
