package com.akiban.cserver;

import com.akiban.message.AkibaSendConnection;
import com.akiban.message.ErrorCode;
import com.akiban.message.ErrorResponse;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.ByteBuffer;

/**
 * A class that buffers exactly one Message. Trying to put more than one message into the buffer will result
 * in an ExceptionResponse being buffered -- as well as appropriate logging.
 */
public class SingleSendBuffer implements AkibaSendConnection {
    static final String NO_MESSAGES_SENT = "No messages sent";
    private static final class BumpMessage extends Message {
        @Override
        public String toString() {
            return "Messages have been bumped because more than one was sent to a SingleSendBuffer.";
        }

        BumpMessage() {
            super((short)-1);
        }

        @Override
        public void read(ByteBuffer payload) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(ByteBuffer payload) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(AkibaSendConnection connection, ExecutionContext context) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean responseExpected() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean errorResponse() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean serializationTestInitialize(int testCaseId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String serializationTestMatch(Message echo) {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * A placeholder message for bumping the last of multiple messages. You should not try to invoke <em>any</em>
     * Message-specific method on this object; they will all throw an UnsupportedOperationException. The standard
     * Object-defined methods will all work, and in particular, toString() will indicate that messages have
     * been bumped.
     * @see #multipleMessages(Message, Message)
     */
    protected final static Message BUMP_MESSAGE = new BumpMessage();

    private static final Log LOG = LogFactory.getLog(SingleSendBuffer.class);
    private Message message = null;
    private String bumpMessage = null;
    private int messagesCount = 0;

    @Override
    final public void send(Message message) {
        if (message == null) {
            throw new NullPointerException("can't buffer null message");
        }
        if (this.message != null) {
            assert messagesCount > 0 : messagesCount;
            bumpMessage = multipleMessages(this.message, message);
        }
        this.message = message;
        ++messagesCount;
    }

    /**
     * Issues a complaint which will form the message of an ExceptionResponse.
     *
     * If more than one message is bumped, invocations of this method will <em>not</em> include the generated
     * ExceptionResponse. However, invoking {@link #getMessage()} will cause the last message to be bumped by
     * a special placeholder Message, {@linkplain #BUMP_MESSAGE}.
     *
     * So, let's say you send messages mOne, mTwo, mThree, and then call getMessage().; You'll see the pairs:
     *  (mOne,mTwo)
     *  (mTwo,mThree)
     *  (mThree, BUMP_MESSAGE)
     * @param newMessage the message that has just been attempted to be sent. Won't be null.
     * @param oldMessage the message that was previously buffered. Won't be null.
     * You can assume this is > 0, since otherwise there's nothing to complain about.
     * @return the complaint message
     */
    protected String multipleMessages(Message oldMessage, Message newMessage) {
        LOG.error(String.format("Bouncing message: (%s) %s", newMessage.getClass(), newMessage));
        final String auxVerb = (messagesCount == 1) ? "is" : "are";
        return String.format("Tried to send a message, but there %s already %s being sent: (%s) %s",
                auxVerb, messagesCount, oldMessage.getClass(), oldMessage);
    }

    /**
     * Returns a message to be sent. There are three scenarios:
     * <ol>
     *  <li>If no messages were sent, an {@linkplain ErrorResponse} is returned with
     *      the message {@value #NO_MESSAGES_SENT}</li>
     *  <li>If multiple messages were sent, an {@linkplain ErrorResponse} is returned with
     *      the message taken from the most recent invocation of {@link #multipleMessages(Message, Message)}.</li>
     *  <li>If one message was sent, it's returned.</li>
     * </ol>
     * @return a message to send back to the listener
     */
    final public Message getMessage() {
        assert (bumpMessage == null) == (messagesCount <= 1)
                : String.format("%d message(s) but bump is %s", messagesCount, bumpMessage);
        if (bumpMessage != null) {
            return new ErrorResponse( ErrorCode.UNEXPECTED_EXCEPTION, multipleMessages(this.message, BUMP_MESSAGE) );
        }
        if (message == null) {
            return new ErrorResponse(ErrorCode.UNEXPECTED_EXCEPTION, NO_MESSAGES_SENT);
        }
        return message;
    }
}
