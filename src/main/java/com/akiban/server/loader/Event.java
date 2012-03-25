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

package com.akiban.server.loader;

import java.sql.Date;

import com.akiban.message.Message;
import com.akiban.message.Sendable;
import com.akiban.util.GrowableByteBuffer;

public class Event implements Sendable
{
    // Sendable interface

    @Override
    public void read(GrowableByteBuffer payload)
    {
        eventId = payload.getInt();
        timestamp = new Date(payload.getLong());
        timeSec = payload.getDouble();
        message = Message.readString(payload);
    }

    @Override
    public void write(GrowableByteBuffer payload)
    {
        payload.putInt(eventId);
        payload.putLong(timestamp.getTime());
        payload.putDouble(timeSec);
        Message.writeString(payload, message);
    }

    // Event interface

    public Event()
    {
    }

    public Event(int eventId, Date timestamp, double timeSec, String message)
    {
        this.eventId = eventId;
        this.timestamp = timestamp;
        this.timeSec = timeSec;
        this.message = message;
    }

    public int eventId()
    {
        return eventId;
    }

    public Date timestamp()
    {
        return timestamp;
    }

    public double timeSec()
    {
        return timeSec;
    }

    public String message()
    {
        return message;
    }

    // State

    private int eventId;
    private Date timestamp;
    private double timeSec;
    private String message;
}
