/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.foundationdb.server.service.externaldata;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.error.QueryTimedOutException;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;

import com.google.inject.Inject;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RowReaderRetryIT extends ITBase
{
    public static final int NROWS = 1000;
    public static final int NTHREADS = 2;
    public static final int COMMIT_FREQUENCY = 100;
    public static final int MAX_RETRIES = 2;
    public static final int FAILURE_RATE = 4;    

    protected ExternalDataService external;
    protected CsvFormat format;
    protected int tableId;
    protected Table table;

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(ExternalDataService.class, FlakeyExternalDataServiceImpl.class);
    }

    @Before
    public void createSchema() {
        external = serviceManager().getServiceByClass(ExternalDataService.class);
        format = new CsvFormat("UTF-8");
            
        tableId = createTable("test", "t", "id INT PRIMARY KEY NOT NULL");
        table = ais().getTable(tableId);
    }

    @Test
    public void loadTest() throws Exception {
        LoadThread[] threads = new LoadThread[NTHREADS];
        for (int j = 0; j < NTHREADS; j++) {
            ByteArrayOutputStream ostr = new ByteArrayOutputStream();
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(ostr, "UTF-8"));
            for (int i = 0; i < NROWS / NTHREADS; i++) {
                pw.println(i * NTHREADS + j);
            }
            pw.close();
            ByteArrayInputStream istr = new ByteArrayInputStream(ostr.toByteArray());
            threads[j] = new LoadThread(istr);
        }
        for (int j = 0; j < NTHREADS; j++) {
            threads[j].start();
        }        
        Thread.sleep(1000);
        long total = 0;
        for (int j = 0; j < NTHREADS; j++) {
            threads[j].join();
            if (threads[j].error != null) throw threads[j].error;
            total += threads[j].count;
        }
        assertEquals(NROWS, total);
        txnService().beginTransaction(session());
        assertEquals(NROWS, getTable(tableId).tableStatus().getRowCount(session()));
        txnService().commitTransaction(session());
    }
    
    class LoadThread extends Thread {
        final InputStream istr;
        final Session session;
        long count;
        Exception error;

        public LoadThread(InputStream istr) {
            this.istr = istr;
            this.session = createNewSession();
        }

        @Override
        public void run() {
            try {
                count = external.loadTableFromCsv(session, istr, format, 0,
                                                  table, table.getColumns(),
                                                  COMMIT_FREQUENCY, MAX_RETRIES,
                                                  null);
            }
            catch (Exception ex) {
                error = ex;
            }
        }
    }

    static class FlakeyExternalDataServiceImpl extends ExternalDataServiceImpl {
        final AtomicInteger counter = new AtomicInteger();

        @Inject
        public FlakeyExternalDataServiceImpl(com.foundationdb.server.service.config.ConfigurationService configService,
                                             com.foundationdb.server.service.dxl.DXLService dxlService, 
                                             com.foundationdb.server.store.Store store,
                                             com.foundationdb.server.service.transaction.TransactionService transactionService,
                                             com.foundationdb.server.service.ServiceManager serviceManager) {
            super(configService, dxlService, store, transactionService, serviceManager);
        }

        @Override
        protected void retryHook(Session session, int i, int maxRetries,
                                 Exception retryException) {
            if ((i < maxRetries) && ((counter.incrementAndGet() % FAILURE_RATE) == 0)) {
                // An isRollbackClass exception not associated with any particular store.
                throw new QueryTimedOutException(0);
            }
            super.retryHook(session, i, maxRetries, retryException);
        }
    }
}
