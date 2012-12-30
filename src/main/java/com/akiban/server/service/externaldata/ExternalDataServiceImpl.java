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

package com.akiban.server.service.externaldata;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.util.AkibanAppender;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;

public class ExternalDataServiceImpl implements ExternalDataService, Service {
    private final ConfigurationService configService;
    private final DXLService dxlService;
    private final Store store;
    private final TransactionService transactionService;
    private final TreeService treeService;
    
    private static final Logger logger = LoggerFactory.getLogger(ExternalDataServiceImpl.class);

    @Inject
    public ExternalDataServiceImpl(ConfigurationService configService,
                                   DXLService dxlService, Store store,
                                   TransactionService transactionService,
                                   TreeService treeService) {
        this.configService = configService;
        this.dxlService = dxlService;
        this.store = store;
        this.transactionService = transactionService;
        this.treeService = treeService;
    }

    /* ExternalDataService */

    @Override
    public void writeBranchAsJson(Session session, PrintWriter writer,
                                  String schemaName, String tableName, 
                                  List<List<String>> keys, int depth) 
            throws IOException {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        UserTable table = ais.getUserTable(schemaName, tableName);
        if (table == null)
            // TODO: Consider sending in-band as JSON.
            throw new NoSuchTableException(schemaName, tableName);
        logger.debug("Writing from {}: {}", table, keys);
        BranchPlanGenerator generator = 
            ais.getCachedValue(this,
                               new CacheValueGenerator<BranchPlanGenerator>() {
                                   @Override
                                   public BranchPlanGenerator valueFor(AkibanInformationSchema ais) {
                                       return new BranchPlanGenerator(ais);
                                   }
                               });
        Operator plan = generator.generate(table);
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = new PersistitAdapter(generator.getSchema(),
                                           store, treeService, 
                                           session, configService);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        PValue pvalue = new PValue(MString.VARCHAR.instance(Integer.MAX_VALUE, false));
        JsonRowWriter json = new JsonRowWriter(table, depth);
        AkibanAppender appender = AkibanAppender.of(writer);
        boolean transaction = false;
        Cursor cursor = null;
        try {
            transactionService.beginTransaction(session);
            transaction = true;
            cursor = API.cursor(plan, queryContext);
            appender.append("[");
            boolean begun = false;
            for (List<String> key : keys) {
                for (int i = 0; i < key.size(); i++) {
                    String akey = key.get(i);
                    // TODO: Check for col=val syntax or have another API?
                    pvalue.putString(akey, null);
                    queryContext.setPValue(i, pvalue);
                }
                if (json.writeRows(cursor, appender, begun ? ",\n" : "\n"))
                    begun = true;
            }
            appender.append(begun ? "\n]" : "]");
            transactionService.commitTransaction(session);
            transaction = false;
        }
        finally {
            if (cursor != null)
                cursor.destroy();
            if (transaction)
                transactionService.rollbackTransaction(session);
        }
    }

    @Override
    public long loadTableFromCsv(Session session, InputStream inputStream, 
                                 CsvFormat format, long skipRows,
                                 UserTable toTable, List<Column> toColumns,
                                 long commitFrequency, QueryContext context) 
            throws IOException {
        DMLFunctions dml = dxlService.dmlFunctions();
        CsvRowReader reader = new CsvRowReader(toTable, toColumns, format, context);
        if (skipRows > 0)
            reader.skipRows(inputStream, skipRows);
        long pending = 0, total = 0;
        boolean transaction = false;
        try {
            NewRow row;
            do {
                row = reader.nextRow(inputStream);
                if (row != null) {
                    logger.trace("Read row: {}", row);
                    if (!transaction) {
                        transactionService.beginTransaction(session);
                        transaction = true;
                    }
                    dml.writeRow(session, row);
                    total++;
                    pending++;
                }
                if ((row == null) ? transaction : (pending >= commitFrequency)) {
                    logger.debug("Committing {} rows", pending);
                    transactionService.commitTransaction(session);
                    transaction = false;
                    pending = 0;
                }
            } while (row != null);
        }
        finally {
            if (transaction)
                transactionService.rollbackTransaction(session);
        }
        return total;
    }

    /* Service */
    
    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void crash() {
    }

}
