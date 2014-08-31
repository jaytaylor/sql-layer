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
package com.foundationdb.server.collation;

import java.nio.charset.Charset;
import java.util.Arrays;

import com.foundationdb.server.types.common.types.StringFactory;
import com.persistit.Key;

public class AkCollatorBinary extends AkCollator {
    private final Charset UTF8 = Charset.forName("UTF8");

    public AkCollatorBinary() {
        super(AkCollatorFactory.UCS_BINARY, 0);
    }
    
    @Override
    public boolean isRecoverable() {
        return true;
    }

    @Override
    public void append(Key key, String value) {
        key.append(value);
    }

    /**
     * Append the given value to the given key.
     */
    public byte[] encodeSortKeyBytes(String value) {
        return value.getBytes(UTF8);
    }

    /**
     * Recover the value or throw an unsupported exception.
     */
    public String debugDecodeSortKeyBytes(byte[] bytes, int index, int length) {
        return internalDecodeSortKeyBytes(bytes, index, length);
    }

    @Override
    public int compare(String string1, String string2) {
        return string1.compareTo(string2);
    }

    @Override
    public boolean isCaseSensitive() {
        return true;
    }

    @Override
    public String toString() {
        return getScheme();
    }

    @Override
    public int hashCode(String string) {
        return string.hashCode();
    }

    @Override
    public int hashCode(byte[] bytes) {
        return hashCode(internalDecodeSortKeyBytes(bytes, 0, bytes.length));
    }

    private String internalDecodeSortKeyBytes(byte[] bytes, int index, int length) {
        return new String(bytes, index, length, UTF8);
    }
}
