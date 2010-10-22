package com.akiban.cserver.loader;

import com.akiban.message.Message;
import com.akiban.message.Sendable;

import java.nio.ByteBuffer;
import java.sql.Date;

public class Event implements Sendable
{
    // Sendable interface

    @Override
    public void read(ByteBuffer payload)
    {
        eventId = payload.getInt();
        timestamp = new Date(payload.getLong());
        timeSec = payload.getDouble();
        message = Message.readString(payload);
    }

    @Override
    public void write(ByteBuffer payload)
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
