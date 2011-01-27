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

package com.akiban.cserver;

import java.nio.ByteBuffer;

import com.akiban.cserver.service.session.Session;
import com.akiban.message.AkibaSendConnection;
import com.akiban.message.Message;
import com.akiban.message.Response;

public class TestResponse extends Response
{
    // Message interface

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
        this.id = payload.getInt();
        this.responseSize = payload.getInt();
        this.payload = new byte[responseSize];
        payload.get(this.payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
        payload.putInt(this.id);
        payload.putInt(this.responseSize);
        payload.put(this.payload);
    }

    @Override
    public void execute(AkibaSendConnection connection, Session session) throws Exception
    {
    }

    // Testable interface

    @Override
    public boolean serializationTestInitialize(int testCaseId)
    {
        return testCaseId == 0;
    }

    @Override
    public String serializationTestMatch(Message echo)
    {
        return null;
    }

    // TestRequest interface

    public int id()
    {
        return id;
    }

    public TestResponse()
    {
        super(TYPE);
    }

    public TestResponse(int id, int responseSize)
    {
        super(TYPE);
        this.id = id;
        this.responseSize = responseSize;
        this.payload = new byte[responseSize];
    }

    // State

    public static short TYPE;

    private int id;
    private int responseSize;
    private byte[] payload;
}
