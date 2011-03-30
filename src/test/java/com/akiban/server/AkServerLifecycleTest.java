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

package com.akiban.server;

import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;

import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.UnitTestServiceFactory;
import com.akiban.message.AkibanConnection;
import com.akiban.message.AkibanConnectionImpl;
import com.akiban.message.MessageRegistry;
import com.akiban.message.MessageRegistryBase;
import com.akiban.util.Command;

public class AkServerLifecycleTest {
    private static final int N = 5;
    private static final int N_REQUESTS = 10;

    private ServiceManager serviceManager;

    private int getNetworkPort() {
        return Integer.parseInt(serviceManager.getConfigurationService().getProperty("akserver.port"));
    }

    @Test
    public void testStartupShutdown() throws Exception {
        for (int i = 0; i < N; i++) {
            startAkServer();
            stopAkServer();
        }
    }

    @Test
    public void testShutdownWithClosedConnections() throws Exception {
        for (int i = 0; i < N; i++) {
            startAkServer();
            try {
                AkibanConnection connection = new AkibanConnectionImpl(
                        LOCALHOST, getNetworkPort());
                for (int r = 0; r < N_REQUESTS; r++) {
                    TestRequest request = new TestRequest(r, 10, 10);
                    TestResponse response = (TestResponse) connection
                            .sendAndReceive(request);
                    Assert.assertEquals(r, response.id());
                }
                connection.close();
            } finally {
                stopAkServer();
            }
        }
    }

    @Test
    public void testShutdownWithOpenConnections() throws Exception {
        for (int i = 0; i < N; i++) {
            startAkServer();
            try {
                AkibanConnection connection = new AkibanConnectionImpl(
                        LOCALHOST, getNetworkPort());
                for (int r = 0; r < N_REQUESTS; r++) {
                    TestRequest request = new TestRequest(r, 10, 10);
                    TestResponse response = (TestResponse) connection
                            .sendAndReceive(request);
                    Assert.assertEquals(r, response.id());
                }
            } finally {
                stopAkServer();
            }
        }
    }

    private void startAkServer() throws Exception {
        MessageRegistry.reset(); // In case a message registry is left over from
                                 // a previous test in the same JVM.
        serviceManager = UnitTestServiceFactory
                .createServiceManagerWithNetworkService();
        final Properties originalProperties = System.getProperties();
        try {
            final Properties testProperties = new Properties(
                    System.getProperties());
            testProperties.setProperty("akiban.admin", "NONE");
            System.setProperties(testProperties);
            serviceManager.startServices();
        } finally {
            System.setProperties(originalProperties);
        }
        Assert.assertTrue(listeningOnPort(getNetworkPort()));
        MessageRegistry.reset();
        initializeMessageRegistry();
    }

    private void stopAkServer() throws Exception {
        final int port = getNetworkPort();
        serviceManager.stopServices();
        serviceManager = null;
        Assert.assertTrue(!listeningOnPort(port));
        MessageRegistry.reset();
    }

    private void initializeMessageRegistry() {
        if (MessageRegistry.only() == null) {
            new TestMessageRegistry();
            MessageRegistry.only().registerModule("com.akiban.message");
            MessageRegistry.only().registerModule("com.akiban.server");
        }
    }

    private boolean listeningOnPort(int portInt) throws Command.Exception,
            IOException {
        String port = Integer.toString(portInt);
        Command command = Command.saveOutput("netstat", "-an");
        command.run();
        boolean up = false;
        for (String line : command.stdout()) {
            if (line.indexOf(port) > 0 && line.indexOf("LISTEN") > 0) {
                up = true;
            }
        }
        return up;
    }

    private static final String LOCALHOST = "127.0.0.1";

    public class TestMessageRegistry extends MessageRegistryBase {
        private TestMessageRegistry() {
            super(10);
            register(1, "com.akiban.server.TestRequest");
            register(2, "com.akiban.server.TestResponse");
        }
    }
}
