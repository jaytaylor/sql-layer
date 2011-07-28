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

package com.akiban.server;

import com.akiban.server.types.ConversionTarget;
import com.persistit.Key;

public final class KeyConversionTarget implements ConversionTarget {

    // KeyConversionTarget interface

    public void attach(Key key) {
        this.key = key;
    }

    @Override
    public void setNull() {
        key.append(null);
    }

    @Override
    public void setLong(long value) {
        key.append(value);
    }

    @Override
    public void setDate(long value) {
        key.append(value);
    }

    @Override
    public void setString(String value) {
        key.append(value);
    }

    // object interface

    @Override
    public String toString() {
        return key.toString();
    }

    // object state

    private Key key;
}
