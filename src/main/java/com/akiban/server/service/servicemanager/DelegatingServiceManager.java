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

package com.akiban.server.service.servicemanager;

import com.akiban.server.AkServerInterface;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.monitor.MonitorService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.sql.pg.PostgresService;

public abstract class DelegatingServiceManager implements ServiceManager {

    // ServiceManager interface

    @Override
    public State getState() {
        return delegate().getState();
    }

    @Override
    public void startServices() throws ServiceStartupException {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public void stopServices() {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public void crashServices() {
        throw new UnsupportedOperationException("can't start services via the static delegate");
    }

    @Override
    public ConfigurationService getConfigurationService() {
        return delegate().getConfigurationService();
    }

    @Override
    public AkServerInterface getAkSserver() {
        return delegate().getAkSserver();
    }

    @Override
    public Store getStore() {
        return delegate().getStore();
    }

    @Override
    public TreeService getTreeService() {
        return delegate().getTreeService();
    }

    @Override
    public PostgresService getPostgresService() {
        return delegate().getPostgresService();
    }

    @Override
    public SchemaManager getSchemaManager() {
        return delegate().getSchemaManager();
    }

    @Override
    public JmxRegistryService getJmxRegistryService() {
        return delegate().getJmxRegistryService();
    }

    @Override
    public StatisticsService getStatisticsService() {
        return delegate().getStatisticsService();
    }

    @Override
    public SessionService getSessionService() {
        return delegate().getSessionService();
    }

    @Override
    public <T> T getServiceByClass(Class<T> serviceClass) {
        return delegate().getServiceByClass(serviceClass);
    }

    @Override
    public DXLService getDXL() {
        return delegate().getDXL();
    }

    @Override
    public boolean serviceIsStarted(Class<?> serviceClass) {
        return delegate().serviceIsStarted(serviceClass);
    }

    @Override
    public MonitorService getMonitorService() {
        return delegate().getMonitorService();
    }

    protected abstract ServiceManager delegate();
}
