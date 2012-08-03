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

package com.akiban.qp.operator;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link QueryContext} for use without a full server for internal plans / testing. */
public class SimpleQueryContext extends QueryContextBase
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleQueryContext.class);

    private StoreAdapter adapter;

    public SimpleQueryContext(StoreAdapter adapter) {
        this.adapter = adapter;
    }

    @Override
    public StoreAdapter getStore() {
        return adapter;
    }

    @Override
    public StoreAdapter getStore(UserTable table) {
        return adapter;
    }
    
    @Override
    public Session getSession() {
    	return adapter.getSession();
    }

    @Override
    public String getCurrentUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSessionUser() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        switch (level) {
        case WARNING:
            logger.warn("{} {}", errorCode, message);
            break;
        case INFO:
            logger.info("{} {}", errorCode, message);
            break;
        case DEBUG:
            logger.debug("{} {}", errorCode, message);
            break;
        }
    }

    @Override
    public void checkQueryCancelation() {
        if (adapter.getSession() != null) {
           super.checkQueryCancelation();
        }
    }

    @Override
    public long sequenceNextValue(TableName sequence) {
        throw new UnsupportedOperationException();
    }
}
