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

package com.foundationdb.util;

import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LoggingStream extends OutputStream
{
    private static final String NL = System.getProperty("line.separator");

    private final Logger log;
    private final boolean isError;
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    private LoggingStream(Logger log, boolean isError) {
        this.log = log;
        this.isError = isError;
    }

    public static LoggingStream forError(Logger log) {
        return new LoggingStream(log, true);
    }

    public static LoggingStream forInfo(Logger log) {
        return new LoggingStream(log, false);
    }


    @Override
    public void write(int b) throws IOException {
        checkOpen();
        buffer.write(b);
    }

    @Override
    public void flush() throws IOException {
        checkOpen();
        if(buffer.size() == 0) {
            return;
        }
        String msg = buffer.toString();
        buffer.reset();
        if(NL.equals(msg)) {
            return;
        }
        if(isError) {
            log.error(msg);
        } else {
            log.info(msg);
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
    }

    private void checkOpen() throws IOException {
        if(buffer == null) {
            throw new IOException("closed");
        }
    }
}
