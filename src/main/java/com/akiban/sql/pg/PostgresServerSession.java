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

package com.akiban.sql.pg;

import com.akiban.sql.parser.SQLParser;

import com.akiban.server.service.session.Session;
import com.akiban.ais.model.AkibanInformationSchema;

import java.util.Properties;
import java.util.Map;

/** A session has the state needed to execute SQL statements and
 * return results to the client. */
public interface PostgresServerSession
{
    /** Return the messenger used to communicate with client. */
    PostgresMessenger getMessenger();

    /** Return the protocol version in use. */
    int getVersion();

    /** Return properties specified by the client. */
    Properties getProperties();

    /** Get a client property. */
    String getProperty(String key);

    /** Get a client property. */
    String getProperty(String key, String defval);

    /** Get session attributes used to store state between statements. */
    Map<String,Object> getAttributes();

    /** Get a session attribute. */
    Object getAttribute(String key);

    /** Set a session attribute. */
    void setAttribute(String key, Object attr);

    /** Return Akiban Server session. */
    Session getSession();

    /** Return server's AIS. */
    AkibanInformationSchema getAIS();
    
}
