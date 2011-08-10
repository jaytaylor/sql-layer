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
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.Quote;
import com.akiban.server.rowdata.RowData;
import com.akiban.util.AkibanAppender;
import com.persistit.Key;

abstract class EncodingBase<T> implements Encoding<T> {
    EncodingBase() {
    }

    protected static long getOffsetAndWidth(FieldDef fieldDef, RowData rowData) {
        return fieldDef.getRowDef().fieldLocation(rowData, fieldDef.getFieldIndex());
    }

    protected static long getCheckedOffsetAndWidth(FieldDef fieldDef, RowData rowData) throws EncodingException {
        final long location = getOffsetAndWidth(fieldDef, rowData);
        if (location == 0) {
            throw new EncodingException("Invalid location for fieldDef in rowData: 0");
        }
        return location;
    }

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
    public void toString(FieldDef fieldDef, RowData rowData, AkibanAppender sb, Quote quote) {
        try {
            sb.append(toObject(fieldDef,rowData));
        } catch (EncodingException e) {
            sb.append("null");
        }
    }

    public T toObject(Key key) {
        Object o = key.decode();
        return getToObjectClass().cast(o);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    // Deprecating methods
    
    abstract public boolean validate(Type type);

    /**
     * Converts the given field to a Java object.
     * @param fieldDef the field within the rowdata to convert
     * @param rowData the rowdata containing the data to decode
     * @return a Java object
     * @throws EncodingException if the rowdata couldn't be converted to the appropriate type
     */
    abstract public T toObject(FieldDef fieldDef, RowData rowData) throws EncodingException;

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
    abstract public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset);

    abstract public int widthFromObject(FieldDef fieldDef, Object value);

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
    abstract public void toKey(FieldDef fieldDef, RowData rowData, Key key);

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
    abstract public void toKey(FieldDef fieldDef, Object value, Key key);
    abstract public long getMaxKeyStorageSize(Column column);
    abstract public Class<T> getToObjectClass();
}
