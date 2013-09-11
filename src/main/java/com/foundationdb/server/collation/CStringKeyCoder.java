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
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.KeyDisplayer;
import com.persistit.encoding.KeyRenderer;
import com.persistit.exception.ConversionException;
import com.persistit.exception.KeyTooLongException;
import com.persistit.util.Util;

/**
 * Helper class that serializes and deserializes CString instances on
 * {@link com.persistit.Key} objects.
 * 
 * @author peter
 * 
 */
public class CStringKeyCoder implements KeyDisplayer, KeyRenderer {

    /**
     * Append an encoded form of the supplied Object which must be a
     * {@link #CString} to the supplied Key. The encoded form is created by a
     * collator; it represents the weight of the string for collation and
     * usually does not include enough information to faithful decode the
     * string.
     * 
     * @param Key
     *            the key
     * @param Object
     *            the CString to append
     * @param CoderContext
     *            not used
     */
    @Override
    public void appendKeySegment(Key key, Object object, CoderContext context) throws ConversionException {
        if (object instanceof CString) {
            CString cs = (CString) object;
            AkCollator collator = AkCollatorFactory.getAkCollator(cs.getCollationId());
            byte[] sortBytes = collator.encodeSortKeyBytes(cs.getString());
            byte[] keyBytes = key.getEncodedBytes();
            int size = key.getEncodedSize();
            if (size + 1 + sortBytes.length > key.getMaximumSize()) {
                throw new KeyTooLongException("Too long: " + size + 1 + sortBytes.length);
            }
            assert cs.getCollationId() > 0 && cs.getCollationId() < AkCollatorFactory.MAX_COLLATION_ID;
            Util.putByte(keyBytes, size, cs.getCollationId());
            System.arraycopy(sortBytes, 0, keyBytes, size + 1, sortBytes.length);
            key.setEncodedSize(size + 1 + sortBytes.length);
        } else {
            throw new ConversionException("Wrong object type: " + (object == null ? null : object.getClass()));
        }
    }

    /**
     * TODO Temporarily returns an approximate version of the string. This is
     * necessary for now to support Index Histograms and Bloom Filters.
     * 
     * @throws ConversionException
     *             because in general CStrings cannot be decoded
     */
    @Override
    public Object decodeKeySegment(Key key, Class<?> clazz, CoderContext context) throws ConversionException {
        // throw new ConversionException("Collated key cannot be decoded");
        return decodeApproximateString(key);
    }

    /**
     * Attempts to make an approximate representation of the encoded string for
     * human-readable displays. For case-insensitive collations the case of the
     * result is likely to be wrong, and for ICU4J collations the result is
     * displayed in hex.
     */
    @Override
    public void displayKeySegment(Key key, Appendable target, Class<?> clazz, CoderContext context)
            throws ConversionException {
        Util.append(target, decodeApproximateString(key));
    }

    /**
     * @throws ConversionException
     *             because in general CStrings cannot be decoded
     */
    @Override
    public void renderKeySegment(Key key, Object object, Class<?> clazz, CoderContext context)
            throws ConversionException {
        throw new ConversionException("Collated key cannot be decoded");
    }

    /**
     * Decode the a String from a key segment based on reverse translation of
     * its sort key weights.
     * 
     * @param key
     * @return
     */
    private String decodeApproximateString(Key key) {
        byte[] rawBytes = key.getEncodedBytes();
        int index = key.getIndex();
        int size = key.getEncodedSize();
        int end = index;
        for (; end < size && rawBytes[end] != 0; end++) {
        }
        if (end - index < 1) {
            throw new ConversionException("CString cannot be decoded");
        }
        int collationId = rawBytes[index] & 0xFF;
        AkCollator collator = AkCollatorFactory.getAkCollator(collationId);
        return collator.decodeSortKeyBytes(rawBytes, index + 1, end - index - 1);
    }

    @Override
    public boolean isZeroByteFree() throws ConversionException {
        return true;
    }
}
