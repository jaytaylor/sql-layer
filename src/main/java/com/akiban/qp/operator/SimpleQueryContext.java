
package com.akiban.qp.operator;

import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.service.ServiceManager;
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
    	if (adapter != null)
            return adapter.getSession();
    	else
            return null;
    }

    @Override
    public ServiceManager getServiceManager() {
        throw new UnsupportedOperationException();
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
    public String getCurrentSchema() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSessionId() {
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

    @Override
    public long sequenceCurrentValue(TableName sequence) {
        throw new UnsupportedOperationException();
    }
}
