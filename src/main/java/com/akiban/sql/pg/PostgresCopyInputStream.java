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

package com.akiban.sql.pg;

import java.io.InputStream;
import java.io.IOException;

public class PostgresCopyInputStream extends InputStream
{
    private PostgresMessenger messenger;
    private byte[] message;
    private int length, pos;
    private boolean eof;
    
    public PostgresCopyInputStream(PostgresMessenger messenger, int ncols) 
            throws IOException {
        this.messenger = messenger;

        messenger.beginMessage(PostgresMessages.COPY_IN_RESPONSE_TYPE.code());
        messenger.writeByte(0); // textual
        messenger.writeShort((short)ncols);
        for (int i = 0; i < ncols; i++) {
            messenger.writeShort(0); // text
        }
        messenger.sendMessage(true);
    }

    @Override
    public int read() throws IOException {
        while (true) {
            if (pos < length)
                return message[pos++];
            if (!nextMessage())
                return -1;
        }
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (len == 0) 
            return 0;
        while (true) {
            int nb = length - pos;
            if (nb > 0) {
                if (nb > len) nb = len;
                System.arraycopy(message, pos, b, off, nb);
                pos += nb;
                return nb;
            }
            if (!nextMessage())
                return -1;
        }
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) return 0;
        long skipped = 0;
        while (true) {
            int nb = length - pos;
            if (nb > 0) {
                if (nb > n) nb = (int)n;
                pos += nb;
                skipped += nb;
                n -= nb;
                if (n <= 0) break;
            }
            if (!nextMessage())
                break;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (length - pos);
    }

    private boolean nextMessage() throws IOException {
        while (true) {
            switch (messenger.readMessage()) {
            case COPY_DATA_TYPE:
                message = messenger.getRawMessage();
                pos = 0;
                length = message.length;
                return true;
            case COPY_DONE_TYPE:
                return false;
            case COPY_FAIL_TYPE:
                throw new IOException("Copy failed: " + messenger.readString());
            case FLUSH_TYPE:
            case SYNC_TYPE:
                break;
            default:
                throw new IOException("Unexpected message type");
            }
        }
    }

}
