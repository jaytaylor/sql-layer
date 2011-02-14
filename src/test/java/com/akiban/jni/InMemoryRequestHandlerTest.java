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

package com.akiban.jni;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.server.service.session.Session;
import com.akiban.message.AkibanConnection;
import com.akiban.message.MessageRegistry;
import com.akiban.message.MessageRegistryBase;
import com.akiban.message.Request;

public final class InMemoryRequestHandlerTest {

    private class TestMessageRegistry extends MessageRegistryBase
    {
        private TestMessageRegistry()
        {
            super(10);
            register(IntegerRequest.TYPE, "com.akiban.jni.IntegerRequest");
            MessageRegistry.only().registerModule("com.akiban.jni");
        }
    }

    @Before
    public void setUp() {
        MessageRegistryBase.reset();
        new TestMessageRegistry();
    }

    @After
    public void tearDown() {
        MessageRegistryBase.reset();
    }

    @Test
    public void getAndCloseConnections() {
        InMemoryRequestHandler handler = new InMemoryRequestHandler();
        long c1 = handler.openConnection();
        long c2 = handler.openConnection();

        if (c1 == c2) {
            fail("two same connections: " + c1);
        }

        assertTrue("c1 close failure", handler.closeConnection(c1));
        assertTrue("c2 close failure", handler.closeConnection(c2));

        assertFalse("c1 close worked but shouldn't have", handler.closeConnection(c1));
        assertFalse("c2 close worked but shouldn't have", handler.closeConnection(c2));
        assertFalse("#3 close worked but shouldn't have", handler.closeConnection(3L));
    }

    private static class InMemoryTestHandler extends InMemoryRequestHandler {
        private final List<Throwable> exceptions = Collections.synchronizedList( new ArrayList<Throwable>() );

        @Override
        public void handleRequest(AkibanConnection connection, Session session, Request request) throws Exception
        {
            try {
                IntegerRequest intRequest = (IntegerRequest) request;
                int payload = intRequest.getTheInt();
                connection.send( new IntegerRequest(payload * 2) );
            }
            catch (Throwable t) {
                exceptions.add(t);
            }
        }

        public void assertNoExceptions() {
            assertEquals("exceptions", Collections.<Throwable>emptyList(), exceptions);
        }
    }

    @Test//(timeout=5000)
    public void requestResponse() throws Throwable {
        final InMemoryTestHandler handler = new InMemoryTestHandler();
        final long handle = handler.openConnection();
        boolean errFound = false;

        try {
            IntegerRequest request = new IntegerRequest(7);
            ByteBuffer requestBuffer = ByteBuffer.allocate(1024);
            request.write(requestBuffer);
            requestBuffer.position(0);

            assertTrue("put", handler.putRequest(handle, requestBuffer));

            IntegerRequest response = new IntegerRequest();
            ByteBuffer responseBuffer = handler.getResponse(handle);
            assertNotNull("response", requestBuffer);
            response.read(responseBuffer);
            assertEquals("response", Integer.valueOf(14), response.getTheInt());

            handler.assertNoExceptions();
        }
        catch (Throwable t) {
            errFound = true;
        }
        finally {
            boolean success = handler.closeConnection(handle);
            if ((!errFound) && (!success)) {
                fail("failed to close handle " + handler);
            }
        }
    }

    @Test
    public void requestResponseGivenOutput() throws Throwable {
        final InMemoryTestHandler handler = new InMemoryTestHandler();
        final long handle = handler.openConnection();
        boolean errFound = false;

        try {
            IntegerRequest request1 = new IntegerRequest(7);
            ByteBuffer request1Buffer = ByteBuffer.allocate(1024);
            request1.write(request1Buffer);
            request1Buffer.position(0);
            // Put a second request as well, to ensure we get the correct order
            IntegerRequest request2 = new IntegerRequest(5);
            ByteBuffer request2Buffer = ByteBuffer.allocate(1024);
            request2.write(request1Buffer);
            request2Buffer.position(0);

            ByteBuffer response1Buffer = ByteBuffer.allocate(1024);
            ByteBuffer response2Buffer = ByteBuffer.allocate(1024);
            assertTrue("put", handler.putRequest(handle, request1Buffer, response1Buffer));
            assertTrue("put", handler.putRequest(handle, request2Buffer, response2Buffer));

            ByteBuffer actualResponse1Buffer = handler.getResponse(handle);
            assertSame("response buffers", response1Buffer, actualResponse1Buffer);

            ByteBuffer actualResponse2Buffer = handler.getResponse(handle);
            assertSame("response buffers", response1Buffer, actualResponse2Buffer);

            handler.assertNoExceptions();
        }
        catch (Throwable t) {
            errFound = true;
        }
        finally {
            boolean success = handler.closeConnection(handle);
            if ((!errFound) && (!success)) {
                fail("failed to close handle " + handler);
            }
        }
    }
}
