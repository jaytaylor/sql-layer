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

package com.foundationdb.server;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.StorageKeySizeExceededException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.persistit.Key;
import com.persistit.exception.KeyTooLongException;

public class PersistitKeyValueTarget implements ValueTarget {

    private final static int DIGEST_SIZE = 16;

    private final static ThreadLocal<MessageDigest> md5Digest = new ThreadLocal<MessageDigest>() {
        public MessageDigest initialValue() {
            try {
                MessageDigest md = MessageDigest.getInstance("MD5");
                return md;
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("No MD5 MessageDigest algorithm available");
            }
        }
    };


    // object state

    private Key key;
    private int maximumKeySegmentLength;
    private Object descForError;
    

    public PersistitKeyValueTarget(Object descForError) {
        this(Integer.MAX_VALUE, descForError);
    }

    public PersistitKeyValueTarget(int max, Object descForError) {
        assert max > DIGEST_SIZE;
        maximumKeySegmentLength = max;
        this.descForError = descForError;
    }
    
    
    // PersistitKeyValueTarget interface

    @Override
    public boolean supportsCachedObjects() {
        return true;
    }

    public void attach(Key key) {
        this.key = key;
    }

    public void attach(Key key, Object descForError) {
        this.key = key;
        this.descForError = descForError;
    }
    
    // ValueTarget interface
    
    @Override
    public TInstance getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull() {
        try {
            key.append(null);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putBool(boolean value) {
        try {
            key.append(value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putInt8(byte value) {
        try {
            key.append((long)value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putInt16(short value) {
        try {
            key.append((long)value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putUInt16(char value) {
        try {
            key.append((long)value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putInt32(int value) {
        try {
            key.append((long)value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putInt64(long value) {
        try {
            key.append(value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putFloat(float value) {
        try {
            key.append(value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putDouble(double value) {
        try {
            key.append(value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putBytes(byte[] value) {
        try {
            key.append(value);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putString(String value, AkCollator collator) {
        try {
            final int size = key.getEncodedSize();
            if (collator == null) {
                key.append(value);
            } else {
                collator.append(key, value);
            }
            digest(size);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putObject(Object object) {
        try {
            final int size = key.getEncodedSize();
            key.append(object);
            digest(size);
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    // object interface

    @Override
    public String toString() {
        return key().toString();
    }

    // for use by this class

    protected final Key key() {
        return key;
    }
    
    private void digest(final int initialSize) {
        final int appended = key.getEncodedSize() - initialSize;
        if (appended <= maximumKeySegmentLength) {
            return;
        }
        MessageDigest md = md5Digest.get();
        md.reset();
        md.update(key.getEncodedBytes(), initialSize, appended);
        final byte[] digest = md.digest();
        assert digest.length == DIGEST_SIZE;
        /*
         * No zeroes allowed in the digest. We simply lose a few bits of
         * fidelity here by replacing any 0 with 0xFF.
         */
        for (int i = 0; i < DIGEST_SIZE; i++) {
            if (digest[i] == 0) {
                digest[i] = (byte)0xFF;
            }
        }
        System.arraycopy(digest, 0, key.getEncodedBytes(),  initialSize + maximumKeySegmentLength - DIGEST_SIZE, DIGEST_SIZE);
        key.setEncodedSize(initialSize + maximumKeySegmentLength + 1);
        key.getEncodedBytes()[key.getEncodedSize() - 1] = 0;
    }

    private void reThrowKeyTooLong(KeyTooLongException e) {
        int max = key.getMaximumSize();
        throw new StorageKeySizeExceededException(e, max, String.valueOf(descForError));
    }
}
