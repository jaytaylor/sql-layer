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

package com.akiban.server.manage;

import java.util.Collection;
import java.util.HashSet;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.AkServer;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.Store;

public class ManageMXBeanImpl implements ManageMXBean {
    private final Store store;
    private final DXLService dxlService;
    private final SessionService sessionService;

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
