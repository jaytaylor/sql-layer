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

package com.akiban.server.collation;

import com.persistit.Key;
import com.persistit.encoding.CoderContext;
import com.persistit.encoding.KeyDisplayer;
import com.persistit.encoding.KeyRenderer;
import com.persistit.exception.ConversionException;
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
            if (size + sortBytes.length + 1 > key.getMaximumSize()) {
                throw new IllegalArgumentException("Too long: " + size + sortBytes.length);
            }
            assert cs.getCollationId() > 0 && cs.getCollationId() < AkCollatorFactory.MAX_COLLATION_ID;
            Util.putByte(keyBytes, size, cs.getCollationId());
            System.arraycopy(sortBytes, 0, keyBytes, size + 1, sortBytes.length - 1);
            key.setEncodedSize(size + sortBytes.length);
        } else {
            throw new ConversionException("Wrong object type: " + (object == null ? null : object.getClass()));
        }
    }

    /**
     * TODO Temporarily returns an approximate version of the string. This is
     * necessary for now to support the Index Histogram code.
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
