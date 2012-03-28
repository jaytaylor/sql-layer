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

package com.akiban.admin;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.akiban.util.GrowableByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.message.Message;
import com.akiban.message.Sendable;

public class Address implements Sendable
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("%s:%s", host.getHostName(), port);
    }

    @Override
    public boolean equals(Object o)
    {
        Address that = (Address) o;
        return this.host.equals(that.host) && this.port == that.port;
    }

    // Sendable interface

    @Override
    public void read(GrowableByteBuffer payload)
    {
        String hostAddress = null;
        try {
            hostAddress = Message.readString(payload);
            host = InetAddress.getByName(hostAddress);
        } catch (UnknownHostException e) {
            logger.error(String.format("Unable to create InetAddress for %s", hostAddress));
        }
        port = payload.getInt();
    }

    @Override
    public void write(GrowableByteBuffer payload)
    {
        Message.writeString(payload, host.getHostAddress());
        payload.putInt(port);
    }

    // Address interface

    public Address(String hostAndPort)
    {
        String[] tokens = hostAndPort.split(":");
        try {
            this.host = InetAddress.getByName(tokens[0]);
        } catch (UnknownHostException e) {
            logger.error(String.format("Caught UnknownHostException trying to resolve host name %s", tokens[0]));
        }
        this.port = Integer.parseInt(tokens[1]);
    }

    public Address()
    {}

    public InetAddress host()
    {
        return host;
    }

    public int port()
    {
        return port;
    }

    // State

    private static final Logger logger = LoggerFactory.getLogger(Address.class);

    private InetAddress host;
    private int port;
}
