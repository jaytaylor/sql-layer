/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.loader;

import java.nio.ByteBuffer;
import java.sql.Date;

import com.akiban.message.Message;
import com.akiban.message.Sendable;

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
