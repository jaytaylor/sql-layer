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

package com.akiban.admin;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

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
    public void read(ByteBuffer payload)
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
    public void write(ByteBuffer payload)
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
