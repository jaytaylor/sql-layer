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

import java.nio.ByteBuffer;

import com.akiban.server.service.session.Session;
import com.akiban.message.AkibanSendConnection;
import com.akiban.message.Message;
import com.akiban.message.Request;

public class TestRequest extends Request
{
    // Object interface

    public String toString()
    {
        return String.format("TestRequest(%s)", id);
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

    public TestRequest()
    {
        super(TYPE);
    }

    public TestRequest(final int id, int requestSize, int responseSize)
    {
        super(TYPE);
        this.id = id;
        this.requestSize = requestSize;
        this.responseSize = responseSize;
        this.payload = new byte[requestSize];
    }

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
        id = payload.getInt();
        requestSize = payload.getInt();
        responseSize = payload.getInt();
        this.payload = new byte[requestSize];
        payload.get(this.payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
        payload.putInt(this.id);
        payload.putInt(this.requestSize);
        payload.putInt(this.responseSize);
        payload.put(this.payload);
    }

    @Override
    public void execute(AkibanSendConnection connection, Session session) throws Exception
    {
        connection.send(new TestResponse(id, responseSize));
    }

    // State

    public static short TYPE;

    private int id;
    private int requestSize;
    private int responseSize;
    private byte[] payload;
}
