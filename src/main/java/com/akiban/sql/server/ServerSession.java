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

import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.t3expressions.OverloadResolver;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.parser.SQLParser;

import com.akiban.sql.optimizer.AISBinderContext;
import com.akiban.sql.optimizer.rule.cost.CostEstimator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.service.instrumentation.SessionTracer;
import com.akiban.server.service.routines.RoutineLoader;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Transaction;

import java.io.IOException;
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

    /** Get a boolean client property with error checking. */
    public boolean getBooleanProperty(String key, boolean defval);

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
    
    /** Return the binder context. */
    public AISBinderContext getBinderContext();
    
    /** Return configured properties. */
    public Properties getCompilerProperties();

    /** Return the object used to trace sessions. */
    public SessionTracer getSessionTracer();

    /** Return an adapter for the session's store. */
    public StoreAdapter getStore();

    /** Return an adapter for the session's store. */
    public StoreAdapter getStore(UserTable table);

    /** Return the tree service. */
    public TreeService getTreeService();

    /** Return the transaction service */
    public TransactionService getTransactionService();

    /** Is a transaction open? */
    public boolean isTransactionActive();

    /** Is a transaction marked rollback-only? */
    public boolean isTransactionRollbackPending();

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

    /** Get query timeout in seconds or <code>null</code> if it has not been set. */
    public long getQueryTimeoutSec();

    /** Get compatibility mode for MySQL zero dates. */
    public ServerValueEncoder.ZeroDateTimeBehavior getZeroDateTimeBehavior();

    /** Send a warning message to the client. */
    public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) throws IOException;

    /** Get the index cost estimator. */
    public CostEstimator costEstimator(ServerOperatorCompiler compiler, KeyCreator keyCreator);

    /** Get the overload resolver */
    public T3RegistryService t3RegistryService();

    /** Get the stored procedure cache */
    public RoutineLoader getRoutineLoader();
}
