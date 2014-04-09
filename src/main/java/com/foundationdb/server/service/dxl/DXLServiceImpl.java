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

package com.foundationdb.server.service.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.error.ServiceNotStartedException;
import com.foundationdb.server.error.ServiceStartupException;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.jmx.JmxManageable;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.statistics.IndexStatisticsService;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class DXLServiceImpl implements DXLService, Service, JmxManageable {
    private final static Logger LOG = LoggerFactory.getLogger(DXLServiceImpl.class);

    // For alterSequence routine
    private final static String SCHEMA = TableName.SYS_SCHEMA;
    private final static String SEQ_RESTART_PROC_NAME = "alter_seq_restart";

    private final Object MONITOR = new Object();

    private volatile HookableDDLFunctions ddlFunctions;
    private volatile DMLFunctions dmlFunctions;
    private volatile boolean didRegister;
    private final SchemaManager schemaManager;
    private final Store store;
    private final SessionService sessionService;
    private final IndexStatisticsService indexStatisticsService;
    private final TypesRegistryService typesRegistry;
    private final TransactionService txnService;
    private final ListenerService listenerService;
    private final ConfigurationService configService;

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("DXL", new DXLMXBeanImpl(this, sessionService), DXLMXBean.class);
    }

    @Override
    public void start() {
        List<DXLFunctionsHook> hooks = getHooks();
        BasicDXLMiddleman middleman = BasicDXLMiddleman.create();
        HookableDDLFunctions localDdlFunctions
                = new HookableDDLFunctions(createDDLFunctions(middleman), hooks,sessionService);
        DMLFunctions localDmlFunctions = new HookableDMLFunctions(createDMLFunctions(middleman, localDdlFunctions),
                hooks, sessionService);
        synchronized (MONITOR) {
            if (ddlFunctions != null) {
                throw new ServiceStartupException("service already started");
            }
            ddlFunctions = localDdlFunctions;
            dmlFunctions = localDmlFunctions;
        }
        registerSystemRoutines();
    }

    DMLFunctions createDMLFunctions(BasicDXLMiddleman middleman, DDLFunctions newlyCreatedDDLF) {
        return new BasicDMLFunctions(middleman, schemaManager, store, newlyCreatedDDLF,
                                     indexStatisticsService, listenerService);
    }

    DDLFunctions createDDLFunctions(BasicDXLMiddleman middleman) {
        return new BasicDDLFunctions(middleman, schemaManager, store, indexStatisticsService,
                typesRegistry, txnService, listenerService, configService);
    }

    @Override
    public void stop() {
        unRegisterSystemRoutines();
        synchronized (MONITOR) {
            if (ddlFunctions == null) {
                throw new ServiceNotStartedException("DDL Functions stop");
            }
            ddlFunctions = null;
            dmlFunctions = null;
            BasicDXLMiddleman.destroy();
        }
    }

    @Override
    public DDLFunctions ddlFunctions() {
        final DDLFunctions ret = ddlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DDL Functions");
        }
        return ret;
    }

    @Override
    public DMLFunctions dmlFunctions() {
        final DMLFunctions ret = dmlFunctions;
        if (ret == null) {
            throw new ServiceNotStartedException("DML Functions");
        }
        return ret;
    }

    protected List<DXLFunctionsHook> getHooks() {
        return Arrays.<DXLFunctionsHook>asList(new DXLTransactionHook(txnService));
    }

    @Override
    public void crash() {
        BasicDXLMiddleman.destroy();
    }

    @Inject
    public DXLServiceImpl(SchemaManager schemaManager, Store store, SessionService sessionService,
                          IndexStatisticsService indexStatisticsService,
                          TypesRegistryService typesRegistry, TransactionService txnService,
                          ListenerService listenerService,
                          ConfigurationService configService) {
        this.schemaManager = schemaManager;
        this.store = store;
        this.sessionService = sessionService;
        this.indexStatisticsService = indexStatisticsService;
        this.typesRegistry = typesRegistry;
        this.txnService = txnService;
        this.listenerService = listenerService;
        this.configService = configService;
    }

    // for use by subclasses

    protected final SchemaManager schemaManager() {
        return schemaManager;
    }

    protected final Store store() {
        return store;
    }

    protected final IndexStatisticsService indexStatisticsService() {
        return indexStatisticsService;
    }

    protected final TypesRegistryService typesRegistry() {
        return typesRegistry;
    }

    protected final TransactionService txnService() {
        return txnService;
    }

    protected final ListenerService listenerService() {
        return listenerService;
    }

    protected final Session session() {
        return null;
    }


    // TODO: Remove when ALTER SEQUENCE is supported directly
    private void registerSystemRoutines() {
        NewAISBuilder builder = AISBBasedBuilder.create(SCHEMA, schemaManager.getTypesRegistry(), schemaManager.getTypesTranslator());
        builder.procedure(SEQ_RESTART_PROC_NAME)
               .language("java", Routine.CallingConvention.JAVA)
               .paramStringIn("schema_name", 128)
               .paramStringIn("sequence_name", 128)
               .paramLongIn("restart_value")
               .returnLong("restart_value")
               .calledOnNullInput(true)
               .externalName(SequenceRoutines.class.getName(), "sequenceRestart");
        AkibanInformationSchema ais = builder.ais();
        Routine routine = ais.getRoutine(SCHEMA, SEQ_RESTART_PROC_NAME);
        schemaManager.registerSystemRoutine(routine);
        didRegister = true;
    }
    private void unRegisterSystemRoutines() {
        if(didRegister) {
            didRegister = false;
            schemaManager.unRegisterSystemRoutine(new TableName(SCHEMA, SEQ_RESTART_PROC_NAME));
        }
    }

    @SuppressWarnings("unused") // Reflectively used
    public static class SequenceRoutines
    {
        public static long sequenceRestart(String schemaName, String sequenceName, long restartValue) {
            ServerQueryContext context = ServerCallContextStack.getCallingContext();
            DXLService dxl = context.getServer().getDXL();
            AkibanInformationSchema ais = dxl.ddlFunctions().getAIS(context.getSession());
            TableName fullName;
            if(schemaName != null) {
                fullName = new TableName(schemaName, sequenceName);
            } else {
                fullName = TableName.parse(context.getCurrentSchema(), sequenceName);
            }
            Sequence curSeq = ais.getSequence(fullName);
            if(curSeq == null) {
                throw new NoSuchSequenceException(schemaName, sequenceName);
            }
            AkibanInformationSchema tempAis = new AkibanInformationSchema();
            Sequence newSeq = Sequence.create(tempAis, fullName.getSchemaName(), fullName.getTableName(), restartValue,
                                              curSeq.getIncrement(), curSeq.getMinValue(), curSeq.getMaxValue(),
                                              curSeq.isCycle());
            dxl.ddlFunctions().alterSequence(context.getSession(), fullName, newSeq);
            return restartValue;
        }
    }
}
