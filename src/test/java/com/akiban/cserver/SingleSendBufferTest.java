package com.akiban.cserver;

import com.akiban.message.ErrorResponse;
import com.akiban.message.Message;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.Assert.*;

public final class SingleSendBufferTest {
    private static class TestSender extends SingleSendBuffer
    {
        private final List<Message> bumped = new ArrayList<Message>();
        private final List<String> bumpMessages = new ArrayList<String>();
        private int messagesCount = 0;

        @Override
        protected String multipleMessages(Message oldMessage, Message newMessage) {
            bumped.add(oldMessage);
            bumped.add(newMessage);
            String message = getBumpMessage(oldMessage, newMessage, ++messagesCount);
            bumpMessages.add(message);
            return String.format("bumped %d", messagesCount);
        }

        public List<Message> getBumped() {
            return Collections.unmodifiableList(bumped);
        }

        public List<String> getBumpMessages() {
            return Collections.unmodifiableList(bumpMessages);
        }
    }

    private static class TestMessageOne extends Message {
        final String message;
        private TestMessageOne(String message) {
            super((short)1);
            this.message = message;
        }

        @Override
        public boolean responseExpected() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return message;
        }
    }

    private static class TestMessageTwo extends TestMessageOne {
        private TestMessageTwo(String message) {
            super(message);
        }
    }

    private static String getBumpMessage(Message prev, Message incoming, int count) {
        return String.format("incoming (%s) \"%s\" bumped (%s) \"%s\" %d", incoming.getClass(), incoming, prev.getClass(), prev, count);
    }

    private static void assertBumped(TestSender sender, Message... expected) {
        List<Message> expectedList = Arrays.asList(expected);
        assertEquals("bumped Message objects", expectedList, sender.getBumped());
    }

    private static void assertBumpMessages(TestSender sender, String... expected) {
        List<String> expectedList = Arrays.asList(expected);
        assertEquals("bump message strings", expectedList, sender.getBumpMessages());
    }

    @Test
    public void noMessagesSent() {
        TestSender sender = new TestSender();

        Message response = sender.getMessage();
        assertEquals("response type", ErrorResponse.class, response.getClass());
        assertEquals("response message", SingleSendBuffer.NO_MESSAGES_SENT, ((ErrorResponse)response).getMessage());

        assertBumped(sender);
        assertBumpMessages(sender);
    }

    @Test
    public void oneMessageSent() {
        TestSender sender = new TestSender();
        Message sendMe = new TestMessageOne("alpha");
        sender.send(sendMe);

        Message response = sender.getMessage();
        assertSame("response", sendMe, response);

        assertBumped(sender);
        assertBumpMessages(sender);
    }

    @Test
    public void twoMessagesSent() {
        TestSender sender = new TestSender();
        Message sendMe1 = new TestMessageOne("1st");
        Message sendMe2 = new TestMessageTwo("2nd");
        sender.send(sendMe1);
        sender.send(sendMe2);

        Message expected = sender.getMessage();
        assertEquals("response type", ErrorResponse.class, expected.getClass());
        assertEquals("response message", "bumped 2", ((ErrorResponse)expected).getMessage());

        assertBumped(sender,
                sendMe1, sendMe2,
                sendMe2, SingleSendBuffer.BUMP_MESSAGE);
        assertBumpMessages(sender,
                getBumpMessage(sendMe1, sendMe2, 1),
                getBumpMessage(sendMe2, SingleSendBuffer.BUMP_MESSAGE, 2));
    }

    @Test
    public void threeMessagesSent() {
        TestSender sender = new TestSender();
        Message sendMe1 = new TestMessageOne("first");
        Message sendMe2 = new TestMessageTwo("second");
        Message sendMe3 = new TestMessageTwo("third");
        sender.send(sendMe1);
        sender.send(sendMe2);
        sender.send(sendMe3);

        Message expected = sender.getMessage();
        assertEquals("response type", ErrorResponse.class, expected.getClass());
        assertEquals("response message", "bumped 3", ((ErrorResponse)expected).getMessage());

        assertBumped(sender,
                sendMe1, sendMe2,
                sendMe2, sendMe3,
                sendMe3, SingleSendBuffer.BUMP_MESSAGE);
        assertBumpMessages(sender,
                getBumpMessage(sendMe1, sendMe2, 1),
                getBumpMessage(sendMe2, sendMe3, 2),
                getBumpMessage(sendMe3, SingleSendBuffer.BUMP_MESSAGE, 3)
        );
    }

    @Test
    public void twoMessagesSentButNoneRetrieved() {
        TestSender sender = new TestSender();
        Message sendMe1 = new TestMessageOne("1st");
        Message sendMe2 = new TestMessageTwo("2nd");
        sender.send(sendMe1);
        sender.send(sendMe2);

        assertBumped(sender,
                sendMe1, sendMe2);
        assertBumpMessages(sender,
                getBumpMessage(sendMe1, sendMe2, 1));
    }

    @Test(expected=NullPointerException.class)
    public void sendNullMessage() {
        SingleSendBuffer sender = new TestSender();
        sender.send(null);
    }
}
