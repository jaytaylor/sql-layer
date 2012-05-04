package com.akiban.collation;

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

    private boolean verifySortByteZeroes(final byte[] a) {
        for (int index = 0; index < a.length - 1; index++) {
            if (a[index] == 0) {
                return false;
            }
        }
        if (a[a.length - 1] != 0) {
            return false;
        }
        return true;
    }

    @Override
    public void appendKeySegment(Key key, Object object, CoderContext context) throws ConversionException {
        if (object instanceof CString) {
            CString cs = (CString) object;
            byte[] sortBytes = cs.getSortKeyBytes();
            byte[] keyBytes = key.getEncodedBytes();
            int size = key.getEncodedSize();
            if (size + sortBytes.length > key.getMaximumSize()) {
                throw new IllegalArgumentException("Too long: " + size + sortBytes.length);
            }
            assert verifySortByteZeroes(sortBytes) : "ICU4J is expected to return a zero-terminated sort key";
            System.arraycopy(sortBytes, 0, keyBytes, size, sortBytes.length);
            key.setEncodedSize(size + sortBytes.length);
        } else {
            throw new ConversionException("Wrong object type: " + (object == null ? null : object.getClass()));
        }
    }

    @Override
    public Object decodeKeySegment(Key key, Class<?> clazz, CoderContext context) throws ConversionException {
        CString cstring = new CString();
        renderKeySegment(key, cstring, clazz, context);
        return cstring;
    }

    @Override
    public void displayKeySegment(Key key, Appendable target, Class<?> clazz, CoderContext context)
            throws ConversionException {
        byte[] rawBytes = key.getEncodedBytes();
        int index = key.getIndex();
        int size = key.getEncodedSize();
        int end = index;
        for (; end < size && rawBytes[end] != 0; end ++) {
        }
        Util.append(target, "CString[");
        Util.bytesToHex(target, rawBytes, index, end - index);
        Util.append(target, "]");
    }

    @Override
    public void renderKeySegment(Key key, Object object, Class<?> clazz, CoderContext context)
            throws ConversionException {
        if (object instanceof CString) {
            CString cs = (CString) object;
            byte[] rawBytes = key.getEncodedBytes();
            int index = key.getIndex();
            int size = key.getEncodedSize();
            int end = index;
            for (; end < size && rawBytes[end] != 0; end ++) {
            }
            cs.putSortKeyBytes(rawBytes, index, end - index + 1);
        } else {
            throw new ConversionException("Wrong object type: " + (object == null ? null : object.getClass()));
        }

    }

}
