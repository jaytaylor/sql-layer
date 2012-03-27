/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.api.dml.scan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.akiban.server.error.RowOutputException;

/**
 * Class for routing a legacy RowData to zero or more handlers. You can use this to route the data to just a single
 * handler (i.e., perform a conversion) or to multiple handlers (in which case it acts as a splitter).
 *
 * <p>This class requires a ByteBuffer to accept each legacy row as it comes in. You can provide one, or have this
 * class allocate one of a given size. If you provide a ByteBuffer, its position at the time that you instantiate this
 * class is considered its base position. After each row is written and routed to all handlers, this class can
 * optionally reset the backing ByteBuffer's position to the initial position. If you use that functionality, a single
 * router can be used to process as many messages as you like, as long as each one fits within the buffer. Otherwise,
 * the buffer will keep filling. You can use this to efficiently split a LegacyRowData, using one of the outputs
 * as the backing ByteBuffer.</p>
 */
public class BufferedLegacyOutputRouter extends WrappingRowOutput {
    public interface Handler {
        void mark();
        void rewind();
        void handleRow(byte[] bytes, int offset, int length);
    }

    private int lastPosition;
    private final boolean resetPosition;
    private final List<Handler> handlers = new ArrayList<Handler>();

    public BufferedLegacyOutputRouter(int capacity, boolean resetPosition) {
        this( byteBuffer(capacity), resetPosition);
    }

    private static ByteBuffer byteBuffer(int capacity) {
        ByteBuffer ret = ByteBuffer.allocate(capacity);
        ret.mark();
        return ret;
    }

    public BufferedLegacyOutputRouter(ByteBuffer buffer, boolean resetPosition) {
        super(buffer);
        if (!buffer.hasArray()) {
            throw new RuntimeException("Buffer needs an array");
        }
        this.resetPosition = resetPosition;
        lastPosition = buffer.position();
    }

    /**
     * Adds a handler to this router. This method returns the handler back to you, as a convenience for when
     * you want to add a new handler and also keep a reference to it.
     * @param handler the handler to add to this router
     * @param <T> the handler's type
     * @return the handler you passed in
     */
    public <T extends Handler> T addHandler(T handler) {
        handlers.add(handler);
        return handler;
    }

    @Override
    protected void postWroteRow() {
        super.postWroteRow();
        final byte[] bytes = wrapped.array();
        final int incomingPosition = wrapped.position();
        final int length = incomingPosition - lastPosition;
        for (Handler handler : handlers) {
            handler.handleRow(bytes, lastPosition, length);
        }
        if (resetPosition) {
            wrapped.position(lastPosition);
        }
        else {
            lastPosition = incomingPosition;
        }
    }

    public void reset(int toBufferPos) {
        wrapped.position(toBufferPos);
        lastPosition = toBufferPos;
    }

    @Override
    public boolean getOutputToMessage()
    {
        return true;
    }

    @Override
    public void mark() {
        super.mark();
        for (Handler handler : handlers) {
            handler.mark();
        }
    }

    @Override
    public void rewind() {
        super.rewind();
        for (Handler handler : handlers) {
            handler.rewind();
        }
    }
}
