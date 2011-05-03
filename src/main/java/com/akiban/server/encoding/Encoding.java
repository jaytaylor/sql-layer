/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.encoding;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Type;
import com.akiban.server.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

public interface Encoding<T> {
    /**
     * Verify that the Encoding enum element is appropriate for the specified
     * Type. Used to assert that the name of the Encoding specified in
     * {@link com.akiban.ais.model.Types} is correct.
     *
     * @param type
     * @return
     */
    boolean validate(final Type type);

    /**
     * Append the value of a field in a RowData to a StringBuilder, optionally
     * quoting string values.
     *
     * @param fieldDef
     *            description of the field
     * @param rowData
     *            RowData containing the data to decode
     * @param sb
     *            The StringBuilder
     * @param quote
     *            Member of the {@link com.akiban.server.Quote} enum that specifies how to add
     *            quotation marks: none, single-quote or double-quote symbols.
     */
    void toString(final FieldDef fieldDef, final RowData rowData, final AkibanAppender sb, final Quote quote);

    /**
     * Converts the given field to a Java object.
     * @param fieldDef the field within the rowdata to convert
     * @param rowData the rowdata containing the data to decode
     * @return a Java object
     * @throws EncodingException if the rowdata couldn't be converted to the appropriate type
     */
    T toObject(final FieldDef fieldDef, final RowData rowData) throws EncodingException;
    
    /**
     * Convert a value supplied as an Object to a value in a RowData backing
     * array. This method is mostly for the convenience of unit tests. It
     * converts a Java Object value of an appropriate type to MySQL format. For
     * example, the DATE Encoding converts an object supplies as a Date, or a
     * String in date format to a MySQL field. For variable-length values
     * (VARCHAR, TEXT, etc.) this method writes the length-prefixed string
     * value.
     *
     * @param fieldDef
     *            description of the field
     * @param value
     *            Value to convert
     * @param dest
     *            Byte array for the RowData being created
     * @param offset
     *            Offset of first byte to write in the array
     * @return number of RowData bytes occupied by the value
     */
    int fromObject(final FieldDef fieldDef, final Object value, final byte[] dest, final int offset);

    /**
     * Size in bytes required by the
     * {@link #fromObject(FieldDef, Object, byte[], int)} method. For
     * fixed-length fields this is the field width. For variable-length fields,
     * this is the number of bytes used to store the item, including the number
     * of prefix bytes used to encode its length. For example, a VARCHAR(300)
     * field containing the ASCII string "abc" requires 5 bytes: 3 for the ASCII
     * characters plus two to encode the length value (3).
     *
     * @param fieldDef
     *            description of the field
     * @param value
     *            the value
     * @return size in bytes
     */
    int widthFromObject(final FieldDef fieldDef, final Object value);

    /**
     * Append a value from a RowData into a Persistit {@link com.persistit.Key},
     * converting the value from its MySQL form to Persistit's key encoding.
     *
     * @param fieldDef
     *            description of the field
     * @param rowData
     *            MySQL data in RowData format
     * @param key
     *            Persistit Key to receive the value
     */
    void toKey(final FieldDef fieldDef, final RowData rowData, final Key key);

    /**
     * Append a value supplied as an Object to a a Persistit
     * {@link com.persistit.Key}.
     *
     * @param fieldDef
     *            description of the field
     * @param value
     *            the value to append
     * @param key
     *            Persistit Key to receive the value
     */
    void toKey(final FieldDef fieldDef, final Object value, final Key key);

    /**
     * Calculate the maximum storage size a given column using this encoding
     * will take when stored in a {@link Key}.
     * @param column column instance
     * @return The maximum storage size.
     */
    long getMaxKeyStorageSize(final Column column);
}
