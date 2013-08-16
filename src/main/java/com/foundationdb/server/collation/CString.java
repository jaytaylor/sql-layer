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

package com.foundationdb.server.collation;

/**
 * Holds a String and a collationId. A custom KeyCoder instance is registered
 * to handle serializing and deserializing instances of this class in a
 * Persistit Key.
 * 
 * This class is not immutable because the CStringKeyCoder decoding methods are
 * designed to populate a hollow instance. However, the mutators should be used
 * only by the CStringKeyCoder and are therefore package-private.
 * 
 * @author peter
 * 
 */
public class CString {

    /**
     * String value
     */
    private String string;

    /**
     * Small integer handle that identifies the collation scheme.
     */
    private int collationId;

    /**
     * Construct an instance containing the original source string. This
     * instance may be used for encoding.
     * 
     * @param string
     * @param collator
     */
    public CString(final String string, final int collationId) {
        this.string = string;
        this.collationId = collationId;
    }

    public CString() {

    }

    public String getString() {
        return string;
    }

    /**
     * @param string
     *            the string to set
     */
    void setString(String string) {
        this.string = string;
    }

    public int getCollationId() {
        return collationId;
    }

    /**
     * @param collationId
     *            the collationId to set
     */
    void setCollationId(int collationId) {
        this.collationId = collationId;
    }

    @Override
    public String toString() {
        return getString();
    }
}
