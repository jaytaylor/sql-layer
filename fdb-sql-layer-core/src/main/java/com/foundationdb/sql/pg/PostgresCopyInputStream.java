/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.pg;

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
