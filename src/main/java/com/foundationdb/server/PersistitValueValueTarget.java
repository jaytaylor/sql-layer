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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueTarget;
import com.persistit.Value;

public final class PersistitValueValueTarget implements ValueTarget {
    
    // PersistitValueValueTarget interface
    
    public void attach(Value value) {
        this.value = value;
    }
    
    // ValueTarget interface
    
    @Override
    public boolean supportsCachedObjects() {
        return false;
    }

    @Override
    public void putObject(Object object) {
        value.put(object);
    }

    @Override
    public TInstance getType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putNull() {
        value.putNull();
    }

    @Override
    public void putBool(boolean value) {
        this.value.put(value);
    }

    @Override
    public void putInt8(byte value) {
        this.value.put(value);
    }

    @Override
    public void putInt16(short value) {
        this.value.put(value);
    }

    @Override
    public void putUInt16(char value) {
        this.value.put(value);
    }

    @Override
    public void putInt32(int value) {
        this.value.put(value);
    }

    @Override
    public void putInt64(long value) {
        this.value.put(value);
    }

    @Override
    public void putFloat(float value) {
        this.value.put(value);
    }

    @Override
    public void putDouble(double value) {
        this.value.put(value);
    }

    @Override
    public void putBytes(byte[] value) {
        this.value.put(value);
    }

    @Override
    public void putString(String value, AkCollator collator) {
        this.value.put(value);
    }
    
    private Value value;
}
