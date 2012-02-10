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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoTransactionInProgressException;
import com.akiban.server.error.TransactionInProgressException;
import com.akiban.server.error.TransactionReadOnlyException;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.SessionTracer;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.sql.optimizer.rule.CostEstimator;
import com.akiban.sql.parser.SQLParser;

import org.joda.time.DateTime;

import java.util.*;

public abstract class ServerSessionBase implements ServerSession
{
    public static final String COMPILER_PROPERTIES_PREFIX = "optimizer.";

    protected final ServerServiceRequirements reqs;
    protected Properties properties;
    protected Map<String,Object> attributes = new HashMap<String,Object>();
    
    protected Session session;
    protected long aisTimestamp = -1;
    protected AkibanInformationSchema ais;
    protected StoreAdapter adapter;
    protected String defaultSchemaName;
    protected SQLParser parser;
    protected ServerTransaction transaction;
    protected boolean transactionDefaultReadOnly = false;
    protected ServerSessionTracer sessionTracer;

    protected ServerValueEncoder.ZeroDateTimeBehavior zeroDateTimeBehavior = ServerValueEncoder.ZeroDateTimeBehavior.NONE;
    protected QueryContext.NotificationLevel maxNotificationLevel = QueryContext.NotificationLevel.INFO;

    public ServerSessionBase(ServerServiceRequirements reqs) {
        this.reqs = reqs;
    }

    @Override
    public Properties getProperties() {
        return properties;
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public String getProperty(String key, String defval) {
        return properties.getProperty(key, defval);
    }

    @Override
    public void setProperty(String key, String value) {
        if (value == null)
            properties.remove(key);
        else
            properties.setProperty(key, value);
        if (!propertySet(key, properties.getProperty(key)))
            sessionChanged();   // Give individual handlers a chance.
    }

    protected void setProperties(Properties properties) {
        this.properties = properties;
        for (String key : properties.stringPropertyNames()) {
            propertySet(key, properties.getProperty(key));
        }
        sessionChanged();
    }

    protected boolean propertySet(String key, String value) {
        if ("zeroDateTimeBehavior".equals(key)) {
            zeroDateTimeBehavior = ServerValueEncoder.ZeroDateTimeBehavior.fromProperty(value);
            return true;
        }
        if ("maxNotificationLevel".equals(key)) {
            maxNotificationLevel = (value == null) ? 
                QueryContext.NotificationLevel.INFO :
                QueryContext.NotificationLevel.valueOf(value);
            return true;
        }
        return false;
    }

    protected abstract void sessionChanged();

    @Override
    public Map<String,Object> getAttributes() {
        return attributes;
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object attr) {
        attributes.put(key, attr);
        sessionChanged();
    }

    @Override
    public DXLService getDXL() {
        return reqs.dxl();
    }

    @Override
    public Session getSession() {
        return session;
    }

    @Override
    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    @Override
    public void setDefaultSchemaName(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
        sessionChanged();
    }

    @Override
    public AkibanInformationSchema getAIS() {
        return ais;
    }

    @Override
    public SQLParser getParser() {
        return parser;
    }
    
    @Override
    public Properties getCompilerProperties() {
        return reqs.config().deriveProperties(COMPILER_PROPERTIES_PREFIX);
    }

    @Override
    public SessionTracer getSessionTracer() {
        return sessionTracer;
     }

    @Override
    public StoreAdapter getStore() {
        return adapter;
    }

    @Override
    public TreeService getTreeService() {
        return reqs.treeService();
    }

    @Override
    public void beginTransaction() {
        if (transaction != null)
            throw new TransactionInProgressException();
        transaction = new ServerTransaction(this, transactionDefaultReadOnly);
    }

    @Override
    public void commitTransaction() {
        if (transaction == null)
            throw new NoTransactionInProgressException();
        try {
            transaction.commit();            
        }
        finally {
            transaction = null;
        }
    }

    @Override
    public void rollbackTransaction() {
        if (transaction == null)
            throw new NoTransactionInProgressException();
        try {
            transaction.rollback();
        }
        finally {
            transaction = null;
        }
    }

    @Override
    public void setTransactionReadOnly(boolean readOnly) {
        if (transaction == null)
            throw new NoTransactionInProgressException();
        transaction.setReadOnly(readOnly);
    }

    @Override
    public void setTransactionDefaultReadOnly(boolean readOnly) {
        this.transactionDefaultReadOnly = readOnly;
    }

    @Override
    public FunctionsRegistry functionsRegistry() {
        return reqs.functionsRegistry();
    }

    @Override
    public Date currentTime() {
        return new Date();
    }

    @Override
    public ServerValueEncoder.ZeroDateTimeBehavior getZeroDateTimeBehavior() {
        return zeroDateTimeBehavior;
    }

    @Override
    public CostEstimator costEstimator() {
        return new ServerCostEstimator(this, reqs);
    }

    /** Prepare to execute given statement.
     * Uses current global transaction or makes a new local one.
     * Returns any local transaction that should be committed / rolled back immediately.
     */
    protected ServerTransaction beforeExecute(ServerStatement stmt) {
        ServerStatement.TransactionMode transactionMode = stmt.getTransactionMode();
        ServerTransaction localTransaction = null;
        if (transaction != null) {
            // Use global transaction.
            transaction.checkTransactionMode(transactionMode);
        }
        else {
            switch (transactionMode) {
            case REQUIRED:
            case REQUIRED_WRITE:
                throw new NoTransactionInProgressException();
            case READ:
            case NEW:
                localTransaction = new ServerTransaction(this, true);
                break;
            case WRITE:
            case NEW_WRITE:
                if (transactionDefaultReadOnly)
                    throw new TransactionReadOnlyException();
                localTransaction = new ServerTransaction(this, false);
                localTransaction.beforeUpdate();
                break;
            }
        }
        return localTransaction;
    }

    /** Complete execute given statement.
     * @see #beforeExecute
     */
    protected void afterExecute(ServerStatement stmt, 
                                ServerTransaction localTransaction,
                                boolean success) {
        if (localTransaction != null) {
            if (success)
                localTransaction.commit();
            else
                localTransaction.abort();
        }
        else {
            // Make changes visible in open global transaction.
            switch (stmt.getTransactionMode()) {
            case REQUIRED_WRITE:
            case WRITE:
                transaction.afterUpdate();
                break;
            }
        }
    }

}
