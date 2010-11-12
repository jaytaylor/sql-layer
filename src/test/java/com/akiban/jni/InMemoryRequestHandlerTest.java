package com.akiban.jni;

import com.akiban.message.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.*;

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
        MessageRegistryBase.shutdown();
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
        public void handleRequest(AkibanConnection connection, Request request) throws Exception {
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
