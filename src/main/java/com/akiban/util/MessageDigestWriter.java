/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.util;

import java.io.IOException;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MessageDigestWriter extends Writer {

    private final MessageDigest digest;
    
    public MessageDigestWriter () throws NoSuchAlgorithmException {
        digest = MessageDigest.getInstance("MD5");
        digest.reset();
    }
    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        byte[] bytes = String.valueOf(cbuf).substring(off, (len+off)).getBytes("UTF-8");
        digest.update(bytes, 0, bytes.length);
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }

    public MessageDigest getDigest() { 
        return digest;
    }
    public String getFormatMD5() {
        return Strings.formatMD5(digest.digest());
    }
}
