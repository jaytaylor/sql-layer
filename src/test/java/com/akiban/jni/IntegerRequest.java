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

package com.akiban.jni;

import java.nio.ByteBuffer;

import com.akiban.message.Request;

public final class IntegerRequest extends Request {

    public IntegerRequest(int initial)
    {
        this();
        theInt = initial;
    }

    public IntegerRequest()
    {
        super(TYPE);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception {
        super.write(payload);
        payload.putInt(theInt);
    }

    @Override
    public void read(ByteBuffer payload) throws Exception {
        super.read(payload);
        theInt = payload.getInt();
    }

    @Override
    public boolean responseExpected() {
        return true;
    }

    public Integer getTheInt() {
        return theInt;
    }

    public static short TYPE;
    private Integer theInt = null;
}
