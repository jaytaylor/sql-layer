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

import com.persistit.Key;

public class AkCollatorBinary extends AkCollator {

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

    @Override
    public String decode(Key key) {
        return key.decodeString();
    }

    /**
     * Append the given value to the given key.
     */
    public byte[] encodeSortKeyBytes(String value) {
        throw new UnsupportedOperationException("No sort key encoding for binary collation");
    }

    /**
     * Recover the value or throw an unsupported exception.
     */
    public String decodeSortKeyBytes(byte[] bytes, int index, int length) {
        throw new UnsupportedOperationException("No sort key encoding for binary collation");
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
        return bytes.hashCode();
    }
}
