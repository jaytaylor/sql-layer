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

package com.akiban.sql.server;

import com.akiban.sql.parser.SQLParser;

import com.akiban.sql.optimizer.rule.IndexEstimator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.loadableplan.LoadablePlan;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.SessionTracer;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;

import java.util.Date;
import java.util.Map;
import java.util.Properties;

/** A session has the state needed to execute SQL statements and
 * return results to the client. */
public interface ServerSession
{
    /** Return properties specified by the client. */
    public Properties getProperties();

    /** Get a client property. */
    public String getProperty(String key);

    /** Get a client property. */
    public String getProperty(String key, String defval);

    /** Set a client property. */
    public void setProperty(String key, String value);

    /** Get session attributes used to store state between statements. */
    public Map<String,Object> getAttributes();

    /** Get a session attribute. */
    public Object getAttribute(String key);

    /** Set a session attribute. */
    public void setAttribute(String key, Object attr);

    /** Return Akiban Server manager. */
    public DXLService getDXL();

    /** Return Akiban Server session. */
    public Session getSession();

    /** Return the default schema for SQL objects. */
    public String getDefaultSchemaName();

    /** Set the default schema for SQL objects. */
    public void setDefaultSchemaName(String defaultSchemaName);

    /** Return server's AIS. */
    public AkibanInformationSchema getAIS();
    
    /** Return a parser for SQL statements. */
    public SQLParser getParser();
    
    /** Return configured properties. */
    public Properties getCompilerProperties();

    /** Return the object used to trace sessions. */
    public SessionTracer getSessionTracer();

    /** Return an adapter for the session's store. */
    public StoreAdapter getStore();

    /** Return the tree service. */
    public TreeService getTreeService();

    /** Return the LoadablePlan with the given name. */
    public LoadablePlan<?> loadablePlan(String planName);

    /** Begin a new transaction. */
    public void beginTransaction();

    /** Commit the current transaction. */
    public void commitTransaction();

    /** Rollback the current transaction. */
    public void rollbackTransaction();

    /** Set current transaction to read-only / read-write. */
    public void setTransactionReadOnly(boolean readOnly);

    /** Set following transaction to read-only / read-write. */
    public void setTransactionDefaultReadOnly(boolean readOnly);

    /** Get the functions registry. */
    public FunctionsRegistry functionsRegistry();

    /** Get the server's idea of the current time. */
    public Date currentTime();

    /** Get compatibilty mode for MySQL zero dates. */
    public ServerValueEncoder.ZeroDateTimeBehavior getZeroDateTimeBehavior();

    /** Get the index estimator. */
    public IndexEstimator indexEstimator();
}
