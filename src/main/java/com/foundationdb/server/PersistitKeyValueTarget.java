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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.StorageKeySizeExceededException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.persistit.Key;
import com.persistit.exception.KeyTooLongException;

public class PersistitKeyValueTarget implements ValueTarget {

    private Key key;
    private Object descForError;

    public PersistitKeyValueTarget(Object descForError) {
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
            if (collator == null) {
                key.append(value);
            } else {
                collator.append(key, value);
            }
        } catch(KeyTooLongException e) {
            reThrowKeyTooLong(e);
        }
    }

    @Override
    public void putObject(Object object) {
        try {
            key.append(object);
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

    private void reThrowKeyTooLong(KeyTooLongException e) {
        int max = key.getMaximumSize();
        throw new StorageKeySizeExceededException(e, max, String.valueOf(descForError));
    }
}
