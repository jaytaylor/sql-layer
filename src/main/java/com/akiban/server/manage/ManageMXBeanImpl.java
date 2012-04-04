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

package com.akiban.server.manage;

import java.util.Collection;
import java.util.HashSet;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.AkServer;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.jmx.JmxManageable.JmxObjectInfo;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.Store;

public class ManageMXBeanImpl implements ManageMXBean {
    private final Store store;
    private final DXLService dxlService;
    private final SessionService sessionService;
    public static final String BEAN_NAME = "com.akiban:type=AKSERVER";

    public ManageMXBeanImpl(Store store, DXLService dxlService, SessionService sessionService) {
        this.store = store;
        this.dxlService = dxlService;
        this.sessionService = sessionService;
    }
    
    @Override
    public void ping() {
        return;
    }

    @Override
    public int getJmxPort() {
        return Integer.getInteger("com.sun.management.jmxremote.port", 0);
    }

    @Override
    public boolean isDeferIndexesEnabled() {
        return getStore().isDeferIndexes();
    }

    @Override
    public void setDeferIndexes(final boolean defer) {
        getStore().setDeferIndexes(defer);
    }

    @Override
    public void buildIndexes(final String arg, final boolean deferIndexes) {
        Session session = createSession();
        try {
            Collection<Index> indexes = gatherIndexes(session, arg);
            getStore().buildIndexes(session, indexes, deferIndexes);
        } catch(Exception t) {
            throw new RuntimeException(t);
        } finally {
            session.close();
        }
    }

    @Override
    public void deleteIndexes(final String arg) {
        Session session = createSession();
        try {
            Collection<Index> indexes = gatherIndexes(session, arg);
            getStore().deleteIndexes(session, indexes);
        } catch(Exception t) {
            throw new RuntimeException(t);
        } finally {
            session.close();
        }
    }

    @Override
    public void flushIndexes() {
        Session session = createSession();
        try {
            getStore().flushIndexes(session);
        } catch(Exception t) {
            throw new RuntimeException(t);
        } finally {
            session.close();
        }
    }

    @Override
    public String getVersionString() {
        return AkServer.VERSION_STRING;
    }

    private Store getStore() {
        return store;
    }

    private Session createSession() {
        return sessionService.createSession();
    }

    /**
     * Test if a given index is selected in the argument string. Format is:
     * <p><code>table=(table_name) index=(index_name)</code></p>
     * This can contain as many table=() and index=() segments as desired.
     * @param index Index to check
     * @param arg Index selection string as described above
     * @return
     */
    private boolean isIndexSelected(Index index, String arg) {
        return (!arg.contains("table=") ||
                arg.contains("table=(" +index.getIndexName().getTableName() + ")"))
               &&
               (!arg.contains("index=") ||
                arg.contains("index=(" + index.getIndexName().getName() + ")"));
    }

    /**
     * Create a collection of all indexes from all user tables in the current AIS
     * that are selected by arg.
     * @param session Session to use
     * @param arg Index selection string
     * @return Collection of selected Indexes
     */
    private Collection<Index> gatherIndexes(Session session, String arg) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Collection<Index> indexes = new HashSet<Index>();
        for(UserTable table : ais.getUserTables().values()) {
            for(Index index : table.getIndexes()) {
                if(isIndexSelected(index, arg)) {
                    indexes.add(index);
                }
            }
        }
        return indexes;
    }

}
