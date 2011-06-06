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

import com.akiban.server.service.servicemanager.configuration.DefaultLockableServiceBinding;
import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.util.Strings;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public final class GuicerTest {

    @Before
    public void clearMessages() {
        DummyInterfaces.clearMessages();
    }

    @Test
    public void simple() throws Exception {
        Guicer<DummyInterfaces.DummyService> guicer = messageGuicer(
                bind(DummyInterfaces.Alpha.class, DummySimpleServices.SimpleAlpha.class, true),
                bind(DummyInterfaces.Beta.class, DummySimpleServices.SimpleBeta.class, false),
                bind(DummyInterfaces.Gamma.class, DummySimpleServices.SimpleGamma.class, false)
        );
        guicer.startAllServices();
        assertEquals(
                "messages",
                joined("starting SimpleGamma", "starting SimpleBeta", "starting SimpleAlpha"),
                Strings.join(DummyInterfaces.messages())
        );
    }

    private static Guicer<DummyInterfaces.DummyService> messageGuicer(ServiceBinding... bindings) throws ClassNotFoundException {
        Guicer<DummyInterfaces.DummyService> guicer = new Guicer<DummyInterfaces.DummyService>(
                Arrays.asList(bindings),
                MESSAGING_ACTIONS
        );
        return onlyGuicer = guicer;
    }

    private static <T> ServiceBinding bind(Class<T> theInterface, Class<? extends T> theClass, boolean required) {
        if (!theInterface.isInterface()) {
            throw new IllegalArgumentException("theInterface must be an interface class: " + theInterface);
        }
        if (theClass.isInterface()) {
            throw new IllegalArgumentException("theClass must not be an interface class: " + theClass);
        }
        ServiceBinding binding = new DefaultLockableServiceBinding(theInterface.getName());
        binding.setImplementingClass(theClass.getName());
        if (required) {
            binding.markDirectlyRequired();
        }
        return binding;
    }

    private static String joined(String... strings) {
        return Strings.join(Arrays.asList(strings));
    }

    // For use within package

    static Guicer<DummyInterfaces.DummyService> onlyGuicer() {
        if (onlyGuicer == null) {
            throw new IllegalStateException("no guicer set");
        }
        return onlyGuicer;
    }

    // class state

    private static final ServiceLifecycleActions<DummyInterfaces.DummyService> MESSAGING_ACTIONS
            = new ServiceLifecycleActions<DummyInterfaces.DummyService>()
    {
        @Override
        public void onStart(DummyInterfaces.DummyService service) throws Exception {
            service.start();
            DummyInterfaces.addMessage("starting " + service.getClass().getSimpleName());
        }

        @Override
        public void onShutdown(DummyInterfaces.DummyService service) throws Exception {
            DummyInterfaces.addMessage("stopping " + service.getClass().getSimpleName());
            service.stop();
        }

        @Override
        public DummyInterfaces.DummyService castIfActionable(Object object) {
            return (object instanceof DummyInterfaces.DummyService) ? (DummyInterfaces.DummyService) object : null;
        }
    };

    private static Guicer<DummyInterfaces.DummyService> onlyGuicer;
}
