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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.dxl.DXLService;
import com.foundationdb.server.service.externaldata.JsonRowWriter.WriteTableRow;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ExternalDataServiceImpl implements ExternalDataService, Service {
    protected final ConfigurationService configService;
    protected final DXLService dxlService;
    protected final Store store;
    protected final TransactionService transactionService;
    protected final ServiceManager serviceManager;
    
    private static final Logger logger = LoggerFactory.getLogger(ExternalDataServiceImpl.class);

    public static final CacheValueGenerator<PlanGenerator> CACHED_PLAN_GENERATOR =
            new CacheValueGenerator<PlanGenerator>() {
                @Override
                public PlanGenerator valueFor(AkibanInformationSchema ais) {
                    return new PlanGenerator(ais);
                }
            };


    @Inject
    public ExternalDataServiceImpl(ConfigurationService configService,
                                   DXLService dxlService, Store store,
                                   TransactionService transactionService,
                                   ServiceManager serviceManager) {
        this.configService = configService;
        this.dxlService = dxlService;
        this.store = store;
        this.transactionService = transactionService;
        this.serviceManager = serviceManager;

    }

    private Table getTable(AkibanInformationSchema ais, String schemaName, String tableName) {
        Table table = ais.getTable(schemaName, tableName);
        if (table == null) {
            // TODO: Consider sending in-band as JSON.
            throw new NoSuchTableException(schemaName, tableName);
        }
        return table;
    }

    private StoreAdapter getAdapter(Session session, Table table, Schema schema) {
        if (table.hasMemoryTableFactory())
            return new MemoryAdapter(schema, session, configService);
        return store.createAdapter(session, schema);
    }

    private TypesTranslator getTypesTranslator() {
        return dxlService.ddlFunctions().getTypesTranslator();
    }

    private void dumpAsJson(Session session,
                            PrintWriter writer,
                            Table table,
                            List<List<Object>> keys,
                            int depth,
                            boolean withTransaction,
                            Schema schema,
                            Operator plan,
                            FormatOptions options) {
        StoreAdapter adapter = getAdapter(session, table, schema);
        QueryContext queryContext = new SimpleQueryContext(adapter) {
                @Override
                public ServiceManager getServiceManager() {
                    return serviceManager;
                }
            };
        QueryBindings queryBindings = queryContext.createBindings();
        JsonRowWriter json = new JsonRowWriter(new TableRowTracker(table, depth));
        WriteTableRow rowWriter = new WriteTableRow();
        AkibanAppender appender = AkibanAppender.of(writer);
        boolean transaction = false;
        Cursor cursor = null;
        try {
            if (withTransaction) {
                transactionService.beginTransaction(session);
                transaction = true;
            }
            cursor = API.cursor(plan, queryContext, queryBindings);
            appender.append("[");
            boolean begun = false;

            if (keys == null) {
                begun = json.writeRows(cursor, appender, "\n", rowWriter, options);
            } else {
                for (List<Object> key : keys) {
                    for (int i = 0; i < key.size(); i++) {
                        ValueSource value = ValueSources.fromObject(key.get(i), null).value();
                        queryBindings.setValue(i, value);
                    }
                    if (json.writeRows(cursor, appender, begun ? ",\n" : "\n", rowWriter, options))
                        begun = true;
                }
            }

            appender.append(begun ? "\n]" : "]");
            if (withTransaction) {
                transactionService.commitTransaction(session);
                transaction = false;
            }
        }
        finally {
            if (cursor != null && !cursor.isClosed())
                cursor.closeTopLevel();
            if (transaction)
                transactionService.rollbackTransaction(session);
        }
    }

    /* ExternalDataService */

    @Override
    public void dumpAllAsJson(Session session, PrintWriter writer,
                              String schemaName, String tableName,
                              int depth, boolean withTransaction, FormatOptions options) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Table table = getTable(ais, schemaName, tableName);
        logger.debug("Writing all of {}", table);
        PlanGenerator generator = ais.getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateScanPlan(table);
        dumpAsJson(session, writer, table, null, depth, withTransaction, generator.getSchema(), plan, options);
    }

    @Override
    public void dumpBranchAsJson(Session session, PrintWriter writer,
                                 String schemaName, String tableName, 
                                 List<List<Object>> keys, int depth,
                                 boolean withTransaction, FormatOptions options) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Table table = getTable(ais, schemaName, tableName);
        logger.debug("Writing from {}: {}", table, keys);
        PlanGenerator generator = ais.getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateBranchPlan(table);
        dumpAsJson(session, writer, table, keys, depth, withTransaction, generator.getSchema(), plan, options);
    }

    @Override
    public void dumpBranchAsJson(Session session, PrintWriter writer,
                                 String schemaName, String tableName, 
                                 Operator scan, RowType scanType, int depth,
                                 boolean withTransaction, FormatOptions options) {
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        Table table = getTable(ais, schemaName, tableName);
        logger.debug("Writing from {}: {}", table, scan);
        PlanGenerator generator = ais.getCachedValue(this, CACHED_PLAN_GENERATOR);
        Operator plan = generator.generateBranchPlan(table, scan, scanType);
        dumpAsJson(session, writer, table, Collections.singletonList(Collections.emptyList()), depth, withTransaction, generator.getSchema(), plan, options);
    }

    @Override
    public long loadTableFromCsv(Session session, InputStream inputStream, 
                                 CsvFormat format, long skipRows,
                                 Table toTable, List<Column> toColumns,
                                 long commitFrequency, int maxRetries,
                                 QueryContext context) 
            throws IOException {
        CsvRowReader reader = new CsvRowReader(toTable, toColumns, inputStream, format,
                                               context, getTypesTranslator());
        if (skipRows > 0)
            reader.skipRows(skipRows);
        return loadTableFromRowReader(session, inputStream, reader, 
                                      commitFrequency, maxRetries);
    }

    @Override
    public long loadTableFromMysqlDump(Session session, InputStream inputStream, 
                                       String encoding,
                                       Table toTable, List<Column> toColumns,
                                       long commitFrequency, int maxRetries,
                                       QueryContext context) 
            throws IOException {
        MysqlDumpRowReader reader = new MysqlDumpRowReader(toTable, toColumns,
                                                           inputStream, encoding, 
                                                           context, getTypesTranslator());
        return loadTableFromRowReader(session, inputStream, reader, 
                                      commitFrequency, maxRetries);
    }

    protected long loadTableFromRowReader(Session session, 
                                          InputStream inputStream, RowReader reader, 
                                          long commitFrequency, int maxRetries)
            throws IOException {
        DMLFunctions dml = dxlService.dmlFunctions();
        long pending = 0, total = 0;
        List<RowData> rowDatas = null;
        if (maxRetries > 0)
            rowDatas = new ArrayList<>();
        boolean transaction = false;
        try {
            NewRow row;
            RowData rowData = null;
            do {
                if (!transaction) {
                    // A transaction is needed, even to read rows, because of auto
                    // increment.
                    transactionService.beginTransaction(session);
                    transaction = true;
                }
                row = reader.nextRow();
                logger.trace("Read row: {}", row);
                if (row != null) {
                    rowData = row.toRowData().copy();
                    if (rowDatas != null) {
                        // Make a copy now so that what we keep is compacter.
                        rowDatas.add(rowData);
                    }
                    total++;
                    pending++;
                }
                boolean commit = false;
                if (row == null) {
                    commit = true;
                }
                else if (commitFrequency == COMMIT_FREQUENCY_PERIODICALLY) {
                    commit = transactionService.shouldPeriodicallyCommit(session);
                }
                else if (commitFrequency != COMMIT_FREQUENCY_NEVER) {
                    commit = (pending >= commitFrequency);
                }
                Exception retryException = null;
                int sessionCounter = -1;
                for (int i = 0; i <= maxRetries; i++) {
                    try {
                        retryHook(session, i, maxRetries, retryException);
                        if (i == 0) {
                            if (row != null) {
                                store.writeRow(session, rowData);
                            }
                        }
                        else {
                            logger.debug("retry #{} from {}", i, retryException);
                            if (!transaction) {
                                transactionService.beginTransaction(session);
                                transaction = true;
                            }
                            if (transactionService.checkSucceeded(session,
                                                                  retryException,
                                                                  sessionCounter)) {
                                logger.debug("transaction had succeeded");
                                rowDatas.clear();
                                break;
                            }
                            // If another exception occurs before here, that is,
                            // while setting up or checking, we repeat check with
                            // original exception and counter. Once check succeeds
                            // but does not pass, we set to get another one.
                            retryException = null;
                            // And errors before another commit cannot be spurious.
                            sessionCounter = -1;
                            for (RowData aRowData : rowDatas) {
                                store.writeRow(session, aRowData);
                            }
                        }
                        if (commit) {
                            if (i == 0) {
                                logger.debug("Committing {} rows", pending);
                                pending = 0;
                            }
                            sessionCounter = transactionService.markForCheck(session);
                            transaction = false;
                            transactionService.commitTransaction(session);
                            if (rowDatas != null) {
                                rowDatas.clear();
                            }
                        }
                        break;
                    }
                    catch (InvalidOperationException ex) {
                        if ((i >= maxRetries) ||
                            !ex.getCode().isRollbackClass()) {
                            throw ex;
                        }
                        if (retryException == null) {
                            retryException = ex;
                        }
                        if (transaction) {
                            transaction = false;
                            transactionService.rollbackTransaction(session);
                        }
                    }
                }
            } while (row != null);
        }
        finally {
            if (transaction) {
                transactionService.rollbackTransaction(session);
            }
        }
        return total;
    }

    
    // For testing by failure injection.
    protected void retryHook(Session session, int i, int maxRetries,
                             Exception retryException) {
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
