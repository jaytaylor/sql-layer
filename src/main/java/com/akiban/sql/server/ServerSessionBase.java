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

package com.akiban.sql.server;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.error.NoTransactionInProgressException;
import com.akiban.server.error.TransactionInProgressException;
import com.akiban.server.error.TransactionReadOnlyException;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.SessionTracer;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.service.tree.TreeService;
import com.akiban.sql.optimizer.AISBinderContext;
import com.akiban.sql.optimizer.rule.cost.CostEstimator;

import java.util.*;

public abstract class ServerSessionBase extends AISBinderContext implements ServerSession
{
    public static final String COMPILER_PROPERTIES_PREFIX = "optimizer.";

    protected final ServerServiceRequirements reqs;
    protected Properties compilerProperties;
    protected Map<String,Object> attributes = new HashMap<String,Object>();
    
    protected Session session;
    protected Map<StoreAdapter.AdapterType, StoreAdapter> adapters = 
        new HashMap<StoreAdapter.AdapterType, StoreAdapter>();
    protected ServerTransaction transaction;
    protected boolean transactionDefaultReadOnly = false;
    protected ServerSessionTracer sessionTracer;

    protected Long queryTimeoutSec = null;
    protected ServerValueEncoder.ZeroDateTimeBehavior zeroDateTimeBehavior = ServerValueEncoder.ZeroDateTimeBehavior.NONE;
    protected QueryContext.NotificationLevel maxNotificationLevel = QueryContext.NotificationLevel.INFO;

    public ServerSessionBase(ServerServiceRequirements reqs) {
        this.reqs = reqs;
    }

    @Override
    public void setProperty(String key, String value) {
        super.setProperty(key, value);
        if (!propertySet(key, properties.getProperty(key)))
            sessionChanged();   // Give individual handlers a chance.
    }

    protected void setProperties(Properties properties) {
        super.setProperties(properties);
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
        if ("queryTimeoutSec".equals(key)) {
            queryTimeoutSec = (value == null) ? null : Long.valueOf(value);
            return true;
        }
        return false;
    }

    @Override
    public void setDefaultSchemaName(String defaultSchemaName) {
        super.setDefaultSchemaName(defaultSchemaName);
        sessionChanged();
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
    public AISBinderContext getBinderContext() {
        return this;
    }

    @Override
    public Properties getCompilerProperties() {
        if (compilerProperties == null)
            compilerProperties = reqs.config().deriveProperties(COMPILER_PROPERTIES_PREFIX);
        return compilerProperties;
    }

    @Override
    public SessionTracer getSessionTracer() {
        return sessionTracer;
     }

    @Override
    public StoreAdapter getStore() {
        return adapters.get(StoreAdapter.AdapterType.PERSISTIT_ADAPTER);
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
    public Long getQueryTimeoutSec() {
        return queryTimeoutSec;
    }

    @Override
    public ServerValueEncoder.ZeroDateTimeBehavior getZeroDateTimeBehavior() {
        return zeroDateTimeBehavior;
    }

    @Override
    public CostEstimator costEstimator(ServerOperatorCompiler compiler, KeyCreator keyCreator) {
        return new ServerCostEstimator(this, reqs, compiler, keyCreator);
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
