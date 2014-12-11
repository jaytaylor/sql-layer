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

package com.foundationdb.server.service.is;

import java.lang.management.ManagementFactory;
import java.rmi.RemoteException;
import java.util.Vector;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.foundationdb.server.service.session.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.google.inject.Inject;
import com.persistit.Management;
import com.persistit.Management.BufferPoolInfo;
import com.persistit.Management.JournalInfo;
import com.persistit.Management.TreeInfo;
import com.persistit.Management.VolumeInfo;
import com.persistit.mxbeans.IOMeterMXBean;

public class StorageSchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service, StorageSchemaTablesService {

    private static final String BASE_PERSITIT_JMX_PATH = "com.persistit:type=Persistit,class=";

    static final TableName STORAGE_ALERTS_SUMMARY = new TableName (SCHEMA_NAME, "storage_alert_summary");
    // STORAGE_ALERTS -> parse String
    static final TableName STORAGE_BUFFER_POOLS = new TableName (SCHEMA_NAME, "storage_buffer_pools");
    static final TableName STORAGE_CHECKPOINT_SUMMARY = new TableName (SCHEMA_NAME, "storage_checkpoint_summary");
    static final TableName STORAGE_CLEANUP_MANAGER_SUMMARY = new TableName (SCHEMA_NAME, "storage_cleanup_manager_summary");
    static final TableName STORAGE_IO_METER_SUMMARY = new TableName (SCHEMA_NAME, "storage_io_meter_summary");
    static final TableName STORAGE_IO_METERS = new TableName (SCHEMA_NAME, "storage_io_meters");
    static final TableName STORAGE_JOURNAL_MANAGER_SUMMARY = new TableName (SCHEMA_NAME, "storage_journal_manager_summary");
    static final TableName STORAGE_MANAGEMENT_SUMMARY = new TableName (SCHEMA_NAME, "storage_management_summary");
    static final TableName STORAGE_TRANSACTION_SUMMARY = new TableName (SCHEMA_NAME, "storage_transaction_summary");
    static final TableName STORAGE_TREES = new TableName (SCHEMA_NAME, "storage_trees");
    static final TableName STORAGE_VOLUMES = new TableName (SCHEMA_NAME, "storage_volumes");

    private final TreeService treeService;
    private MBeanServer jmxServer;
    
    private final static Logger logger = LoggerFactory.getLogger(StorageSchemaTablesServiceImpl.class);
    
    @Inject
    public StorageSchemaTablesServiceImpl (SchemaManager schemaManager, TreeService treeService) {
        super(schemaManager);
        this.treeService = treeService;
    }

    @Override
    public void start() {
        logger.debug("Starting Storage Schema Tables Service");
        jmxServer = ManagementFactory.getPlatformMBeanServer();

        AkibanInformationSchema ais = createTablesToRegister(schemaManager.getTypesTranslator());
        
        //STORAGE_ALERTS_SUMMARY
        attach (ais, STORAGE_ALERTS_SUMMARY, AlertSummaryFactory.class);
        
        // STORAGE_BUFFER_POOLS
        attach (ais, STORAGE_BUFFER_POOLS, BufferPoolFactory.class);
        
        //STORAGE_CHECKPOINT_SUMMARY
        attach (ais, STORAGE_CHECKPOINT_SUMMARY, CheckpointSummaryFactory.class);

        //STORAGE_CLEANUP_MANAGER_SUMMARY
        attach (ais, STORAGE_CLEANUP_MANAGER_SUMMARY, CleanupSummaryFactory.class);
        
        //STORAGE_IO_METER_SUMMARY
        attach (ais, STORAGE_IO_METER_SUMMARY, IoSummaryFactory.class);

        // STORAGE_IO_METERS
        attach(ais, STORAGE_IO_METERS, IOMetersFactory.class);

        //STORAGE_JOURNAL_MANAGER_SUMMARY
        attach (ais, STORAGE_JOURNAL_MANAGER_SUMMARY, JournalManagerFactory.class);

        //STORAGE_MANAGEMENT_SUMMARY
        attach(ais, STORAGE_MANAGEMENT_SUMMARY, ManagementSummaryFactory.class);

        //STORAGE_TRANSACTION_SUMMARY
        attach (ais, STORAGE_TRANSACTION_SUMMARY, TransactionSummaryFactory.class);

        //STORAGE_TREES
        attach (ais, STORAGE_TREES, TreesFactory.class);

        //STORAGE_VOLUMES
        attach (ais, STORAGE_VOLUMES, VolumesFactory.class);
    }

    @Override
    public void stop() {
        jmxServer = null;
    }

    @Override
    public void crash() {
        // nothing
    }
    
    private ObjectName getBeanName (String name) {
        ObjectName mbeanName = null;
        try {
            mbeanName = new ObjectName (BASE_PERSITIT_JMX_PATH + name);
        } catch (MalformedObjectNameException e) {
            logger.error ("Using " + name + " throws MalformedObjectNameException: " + e.getMessage());
        }
        return mbeanName;
    }
    private Object getJMXAttribute (ObjectName mbeanName,  String attributeName) {
        Object value = null;
        try {
            value = jmxServer.getAttribute(mbeanName, attributeName);
        } catch (AttributeNotFoundException e) {
            logger.error (mbeanName.toString() + "#" + attributeName + " not found. Error: " + e.toString());
        } catch (InstanceNotFoundException e) {
            logger.error(jmxServer.toString() + " JMX instance not found. Error: " + e.toString());
        } catch (MBeanException e) {
            logger.error("Mbean retrival: " + mbeanName + " generated error: " + e.getMessage());
        } catch (ReflectionException e) {
            logger.error("Unexepcted reflection error: " + e.getMessage());
        }
        return value;
    }
    
    private Object getJMXInvoke (ObjectName mbeanName, String methodName, Object[] parameters) {
        Object value = null;
        Vector<String>signature = new Vector<>(parameters.length);
        String[] sig = new String[parameters.length];
        
        for (Object param : parameters) {
            signature.add(param.getClass().getName());
        }
        
        try {
            value = jmxServer.invoke(mbeanName, methodName, parameters, signature.toArray(sig));
        } catch (InstanceNotFoundException e) {
            logger.error(jmxServer.toString() + " JMX instance not found. Error: " + e.toString());
        } catch (ReflectionException e) {
            logger.error("Unexepcted reflection error: " + e.getMessage());
        } catch (MBeanException e) {
            logger.error("Unexpected MBeanException: " + e.getMessage());
        }
        
        return value;
    }
    
    private abstract class BeanScan extends BaseScan {
        ObjectName mbeanName;
        
        public BeanScan (RowType rowType, String beanName) {
            super(rowType);
            mbeanName = getBeanName(beanName);
        }
    }

    private class AlertSummaryFactory extends BasicFactoryBase {
        public AlertSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
        private class Scan extends BeanScan {
            public Scan(RowType rowType) {
                super (rowType, "AlertMonitor");
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        getJMXAttribute (mbeanName, "AlertLevel"),
                        getJMXAttribute (mbeanName, "WarnLogTimeInterval"),
                        getJMXAttribute (mbeanName, "ErrorLogTimeInterval"),
                        getJMXAttribute (mbeanName, "HistoryLength"),
                        ++rowCounter);
            }
        }
    }
    
    private class BufferPoolFactory extends BasicFactoryBase {
        public BufferPoolFactory(TableName sourceTable) {
            super(sourceTable);
        }
        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            try {
                return treeService.getDb().getManagement().getBufferPoolInfoArray().length;
            } catch (RemoteException e) {
                return 0;
            }
        }
        
        private class Scan extends BeanScan {
            BufferPoolInfo[] bufferPools = null;
            int bufferPoolCounter = 0;
            public Scan (RowType rowType) {
                super(rowType, "BufferPool");
                
                try {
                    bufferPools = treeService.getDb().getManagement().getBufferPoolInfoArray();
                } catch (RemoteException e) {
                    logger.error ("Unable to obtain the buffer pool info array: " + e.getMessage());
                }
            }

            @Override
            public Row next() {
                if (bufferPools == null) {
                    return null;
                }
                if (rowCounter >= bufferPools.length) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        bufferPools[bufferPoolCounter].getBufferSize(),
                        bufferPools[bufferPoolCounter].getBufferCount(),
                        bufferPools[bufferPoolCounter].getValidPageCount(),
                        bufferPools[bufferPoolCounter].getDirtyPageCount(),
                        bufferPools[bufferPoolCounter].getReaderClaimedPageCount(),
                        bufferPools[bufferPoolCounter].getWriterClaimedPageCount(),
                        bufferPools[bufferPoolCounter].getHitCount(),
                        bufferPools[bufferPoolCounter].getMissCount(),
                        bufferPools[bufferPoolCounter].getNewCount(),
                        bufferPools[bufferPoolCounter].getEvictCount(),
                        bufferPools[bufferPoolCounter].getWriteCount(),
                        bufferPools[bufferPoolCounter].getForcedCheckpointWriteCount(),
                        bufferPools[bufferPoolCounter++].getForcedWriteCount(),
                        ++rowCounter);
                
            }
        }
    }
    
    private class CheckpointSummaryFactory extends BasicFactoryBase {

        public CheckpointSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
       
        private class Scan extends BeanScan{
            public Scan (RowType rowType) {
                super (rowType, "CheckpointManager");
             }
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                            getJMXAttribute(mbeanName, "CheckpointInterval"),
                            ++rowCounter /* Hidden PK */);
            }
        }
    }
    
    private class CleanupSummaryFactory extends BasicFactoryBase {
        public CleanupSummaryFactory (TableName sourceTable) {
            super (sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
        private class Scan extends BeanScan {
            public Scan (RowType rowType) {
                super (rowType, "CleanupManager");
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        getJMXAttribute(mbeanName, "AcceptedCount"),
                        getJMXAttribute(mbeanName, "RefusedCount"),
                        getJMXAttribute(mbeanName, "PerformedCount"),
                        getJMXAttribute(mbeanName, "ErrorCount"),
                        getJMXAttribute(mbeanName, "EnqueuedCount"),
                        getJMXAttribute(mbeanName, "PollInterval"),
                        getJMXAttribute(mbeanName, "MinimumPruningDelay"),
                        ++rowCounter);
            }
        }
    }
    
    private class IoSummaryFactory extends BasicFactoryBase {
        public IoSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
        private class Scan extends BeanScan{
            public Scan (RowType rowType) {
                super (rowType, "IOMeter");
            }
            
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        getJMXAttribute(mbeanName, "IoRate"),
                        getJMXAttribute(mbeanName, "QuiescentIOthreshold"),
                        getJMXAttribute(mbeanName, "LogFile"),
                        ++rowCounter);
            }
        }
    }
    
    private class IOMetersFactory extends BasicFactoryBase {

        public IOMetersFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return IOMeterMXBean.OPERATIONS.length - 1;
        }
        
        private class Scan extends BeanScan {
            Vector<String> parameter;
            public Scan(RowType rowType) {
                super(rowType, "IOMeter");
                parameter = new Vector<>(1);
                parameter.add(IOMeterMXBean.OPERATIONS[0]);
            }

            @Override
            public Row next() {
                if (rowCounter >= IOMeterMXBean.OPERATIONS.length - 1) {
                    return null;
                }
                parameter.set(0, IOMeterMXBean.OPERATIONS[(int)rowCounter+1]);
                return new ValuesHolderRow(rowType,
                        IOMeterMXBean.OPERATION_NAMES[(int)rowCounter+1],
                        getJMXInvoke (mbeanName, "totalBytes", parameter.toArray()),
                        getJMXInvoke (mbeanName, "totalOperations", parameter.toArray()),
                        ++rowCounter);
            }
        }
    }
    
    private class JournalManagerFactory extends BasicFactoryBase {
        public JournalManagerFactory (TableName sourceTable) {
            super (sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
        private class Scan extends BeanScan {
            JournalInfo journal;
            public Scan (RowType rowType) {
                super(rowType, "JournalManager");
                try {
                    journal = treeService.getDb().getManagement().getJournalInfo();
                } catch (RemoteException e) {
                    logger.error("Unable to get Journal Info: " + e.getMessage());
                    journal = null;
                }
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                String[] params = new String[0]; 
                               
                return new ValuesHolderRow(rowType,
                        journal.getBlockSize(),
                        journal.getBaseAddress(),
                        journal.getCurrentJournalAddress(),
                        journal.getPageMapSize(),
                        journal.getRecoveryStatus(),
                        journal.getJournaledPageCount(),
                        journal.getCopiedPageCount(),
                        journal.getDroppedPageCount(),
                        journal.getReadPageCount(),
                        boolResult(journal.isAppendOnly()),
                        boolResult(journal.isFastCopying()),
                        boolResult(journal.isCopying()),
                        boolResult(journal.isFlushing()),
                        journal.getLastValidCheckpointSystemTime()/1000,
                        
                        getJMXAttribute (mbeanName, "PageListSize"),
                        getJMXAttribute(mbeanName, "FlushInterval"),
                        getJMXAttribute(mbeanName, "CopierInterval"),
                        getJMXInvoke(mbeanName, "urgency", params),
                        getJMXAttribute(mbeanName, "TotalCompletedCommits"),
                        getJMXAttribute(mbeanName, "CommitCompletionWaitTime"),
                        getJMXAttribute(mbeanName, "SlowIoAlertThreshold"),
                        boolResult((Boolean) getJMXAttribute(mbeanName, "RollbackPruningEnabled")),
                        getJMXAttribute(mbeanName, "JournalFilePath"),
                        ((Long)getJMXAttribute(mbeanName, "JournalCreatedTime")).longValue()/1000,
                        ++rowCounter);
            }
        }
    }

    //STORAGE_MANAGEMENT_SUMMARY
    private class ManagementSummaryFactory extends BasicFactoryBase {

        public ManagementSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
        private class Scan extends BeanScan {
            Management db_manage;
            public Scan(RowType rowType) {
                super(rowType, "Management");
                
                db_manage = treeService.getDb().getManagement();
            }
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                ValuesHolderRow row;
                try {
                    row = new ValuesHolderRow(rowType,
                            boolResult(db_manage.isInitialized()),
                            boolResult(db_manage.isUpdateSuspended()),
                            boolResult(db_manage.isShutdownSuspended()),
                            db_manage.getVersion(),
                            db_manage.getCopyright(),
                            db_manage.getStartTime()/1000,
                            db_manage.getDefaultCommitPolicy(),
                            ++rowCounter);
                } catch (RemoteException e) {
                    logger.error ("Getting Manager items throws exception: " + e.getMessage());
                    return null;
                }
                return row;
            }            
        }
    }
    
    //STORAGE_TRANSACTION_SUMMARY
    private class TransactionSummaryFactory extends BasicFactoryBase {

        public TransactionSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return 1;
        }
        
        private class Scan extends BeanScan {
            public Scan(RowType rowType) {
                super(rowType, "TransactionIndex");
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        getJMXAttribute(mbeanName, "ActiveTransactionFloor"),
                        getJMXAttribute(mbeanName, "ActiveTransactionCeiling"),
                        getJMXAttribute(mbeanName, "ActiveTransactionCount"),
                        getJMXAttribute(mbeanName, "CurrentCount"),
                        getJMXAttribute(mbeanName, "LongRunningCount"),
                        getJMXAttribute(mbeanName, "AbortedCount"),
                        getJMXAttribute(mbeanName, "FreeCount"),
                        getJMXAttribute(mbeanName, "DroppedCount"),
                        ++rowCounter);
            }
        }
    }
    
    //STORAGE_TREES
    private class TreesFactory extends BasicFactoryBase {

        public TreesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long rows = 0;
            VolumeInfo[] volumes;
            try {
                volumes = treeService.getDb().getManagement().getVolumeInfoArray();
                for (VolumeInfo v : volumes) {
                    rows += treeService.getDb().getManagement().getTreeInfoArray(v.getName()).length;
                }
            } catch (RemoteException e) {
                logger.error("Unable to retrieve volume and tree information: " + e.getMessage());
            }
            return rows;
        }
        private class Scan extends BeanScan {
            VolumeInfo[] volumes;
            int volumeIndex = 0;
            TreeInfo[] trees = null;
            int treeIndex = 0;
            
            public Scan(RowType rowType) {
                super(rowType, "TreeInfo");
                try {
                    volumes = treeService.getDb().getManagement().getVolumeInfoArray();
                    trees = treeService.getDb().getManagement().getTreeInfoArray(volumes[0].getName());
                } catch (RemoteException e) {
                    logger.error("Unable to retrieve volumne information: " + e.getMessage());
                }
            }

            @Override
            public Row next() {
                ValuesHolderRow row;
                if (volumes == null) {
                    return null;
                }
                if (volumeIndex >= volumes.length) {
                    return null;
                }
                if (trees == null) {
                    return null;
                }

                row = new ValuesHolderRow(rowType,
                        volumes[volumeIndex].getName(),
                        trees[treeIndex].getName(),
                        trees[treeIndex].getStatus(),
                        (long)trees[treeIndex].getDepth(),
                        trees[treeIndex].getFetchCounter(),
                        trees[treeIndex].getTraverseCounter(),
                        trees[treeIndex].getStoreCounter(),
                        trees[treeIndex].getRemoveCounter(),
                        ++rowCounter);

                if (++treeIndex >= trees.length) {
                    if (++volumeIndex >= volumes.length) {
                        trees = null;
                    } else {
                        try {
                            trees = treeService.getDb().getManagement().getTreeInfoArray(volumes[volumeIndex].getName());
                            treeIndex = 0;
                        } catch (RemoteException e) {
                            logger.error("Unable to retrieve tree information: " + e.getMessage());
                            trees = null;
                        }
                    }
                }
                return row;
            }
        }
    }
    //STORAGE_VOLUMES
    private class VolumesFactory extends BasicFactoryBase {

        public VolumesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            try {
                return treeService.getDb().getManagement().getVolumeInfoArray().length;
            } catch (RemoteException e) {
                logger.error("Unable to get VolumeInfo array: " + e.getMessage());
                return 0;
            }
        }

        private class Scan extends BeanScan {
            VolumeInfo[] volumes;
            int volumeRowCounter = 0;
            public Scan(RowType rowType) {
                super(rowType, "Volumes");
                try {
                    volumes = treeService.getDb().getManagement().getVolumeInfoArray();
                } catch (RemoteException e) {
                    logger.error("Unable to get VolumeInfo array: " + e.getMessage());
                    volumes = null;
                }
            }

            @Override
            public Row next() {
                if (volumes == null) {
                    return null;
                }
                if (rowCounter >= volumes.length) {
                    return null;
                }
                
                ValuesHolderRow row = new ValuesHolderRow(rowType,
                        volumes[volumeRowCounter].getName(),
                        volumes[volumeRowCounter].getPath(),
                        boolResult(volumes[volumeRowCounter].isTransient()),
                        volumes[volumeRowCounter].getPageSize(),
                        volumes[volumeRowCounter].getCurrentSize(),
                        volumes[volumeRowCounter].getMaximumSize(),
                        volumes[volumeRowCounter].getExtensionSize(),
                        volumes[volumeRowCounter].getCreateTime().getTime()/1000,
                        volumes[volumeRowCounter].getOpenTime().getTime()/1000,
                        volumes[volumeRowCounter].getLastReadTime().getTime()/1000,
                        volumes[volumeRowCounter].getLastWriteTime().getTime()/1000,
                        volumes[volumeRowCounter].getLastExtensionTime().getTime()/1000,
                        volumes[volumeRowCounter].getGeneration(),
                        volumes[volumeRowCounter].getGetCounter(),
                        volumes[volumeRowCounter].getReadCounter(),
                        volumes[volumeRowCounter].getWriteCounter(),
                        volumes[volumeRowCounter].getFetchCounter(),
                        volumes[volumeRowCounter].getTraverseCounter(),
                        volumes[volumeRowCounter].getStoreCounter(),
                        volumes[volumeRowCounter].getRemoveCounter(),
                        rowCounter);
                ++rowCounter;
                ++volumeRowCounter;
                return row;
            }
        }
    }
    static AkibanInformationSchema createTablesToRegister(TypesTranslator typesTranslator) {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator);
        
        builder.table(STORAGE_ALERTS_SUMMARY)
            .colString("alert_level", DESCRIPTOR_MAX, false)
            .colBigInt("warn_log_interval", false)
            .colBigInt("error_log_interval", false)
            .colBigInt("history_length", false);
        
        
        builder.table(STORAGE_BUFFER_POOLS)
            .colBigInt("buffer_size", false)
            .colBigInt("buffer_count", false)
            .colBigInt("valid_pages", false)
            .colBigInt("dirty_pages", false)
            .colBigInt("reader_claimed_pages", false)
            .colBigInt("writer_claimed_pages", false)
            .colBigInt("hit_count", false)
            .colBigInt("miss_count", false)
            .colBigInt("new_count", false)
            .colBigInt("evict_count", false)
            .colBigInt("write_count", false)
            .colBigInt("forced_chkpt_count", false)
            .colBigInt("forced_write_count", false);
            
        builder.table(STORAGE_CHECKPOINT_SUMMARY)
            .colBigInt("checkpoint_interval", false);

        
        builder.table(STORAGE_CLEANUP_MANAGER_SUMMARY)
            .colBigInt ("accepted_count", false)
            .colBigInt ("refused_count", false)
            .colBigInt("performed_count", false)
            .colBigInt("error_count", false)
            .colBigInt("enqueued_count", false)
            .colBigInt("poll_interval", false)
            .colBigInt("min_prune_delay", false);

        builder.table(STORAGE_IO_METER_SUMMARY)
            .colBigInt("io_rate", false)
            .colBigInt("quiescent_threshold", false)
            .colString("log_file", PATH_MAX);
        
        builder.table(STORAGE_IO_METERS)
            .colString("operation", DESCRIPTOR_MAX, false)
            .colBigInt("total_bytes", false)
            .colBigInt("operations", false);
        
        builder.table(STORAGE_JOURNAL_MANAGER_SUMMARY)
            .colBigInt("block_size", false)
            .colBigInt("base_address", false)
            .colBigInt("current_address", false)
            .colBigInt("page_map_size", false)
            .colBigInt("recovery_status", false)
            .colBigInt("journaled_page_count", false)
            .colBigInt("copied_page_count", false)
            .colBigInt("dropped_page_count", false)
            .colBigInt("read_page_count", false)
            .colString("append_only", YES_NO_MAX, false)
            .colString("fast_copy", YES_NO_MAX, false)
            .colString("copy_active", YES_NO_MAX, false)
            .colString("flush_active", YES_NO_MAX, false)
            .colSystemTimestamp("checkpoint_time", false)
            
            .colBigInt("page_list_size", false)
            .colBigInt("flush_interval", false)
            .colBigInt("copier_interval", false)
            .colBigInt("copier_urgency", false)
            .colBigInt("total_commits", false)
            .colBigInt("commit_wait_time", false)
            .colBigInt("slow_alert_threshold", false)
            .colString("rollback_pruning_enabled", YES_NO_MAX, false)

            .colString("file_path", PATH_MAX, false)
            .colSystemTimestamp("create_time", false);

        
        builder.table(STORAGE_MANAGEMENT_SUMMARY)
            .colString("initialized", YES_NO_MAX, false)
            .colString("update_suspended", YES_NO_MAX, false)
            .colString("shutdown_suspended", YES_NO_MAX, false)
            .colString("version", IDENT_MAX, false)
            .colString("copyright", IDENT_MAX, false)
            .colSystemTimestamp("start_time", false)
            .colString("default_commit_policy", DESCRIPTOR_MAX, false);
        
        builder.table(STORAGE_TRANSACTION_SUMMARY)
            .colBigInt("active_floor", false)
            .colBigInt("active_ceiling", false)
            .colBigInt("active_count", false)
            .colBigInt("current_count", false)
            .colBigInt("long_running_count", false)
            .colBigInt("aborted_count", false)
            .colBigInt("free_count",false)
            .colBigInt("dropped_count", false);

        builder.table(STORAGE_TREES)
            .colString("volume_name", IDENT_MAX, false)
            .colString("tree_name", IDENT_MAX, false)
            .colString("status", DESCRIPTOR_MAX, false)
            .colBigInt("depth", false)
            .colBigInt("fetch_counter", false)
            .colBigInt("traverse_counter", false)
            .colBigInt("store_counter", false)
            .colBigInt("remove_counter", false);
            
        builder.table(STORAGE_VOLUMES)
            .colString("volume_name", IDENT_MAX, false)
            .colString("path", PATH_MAX, false)
            .colString("temporary", YES_NO_MAX, false)
            .colBigInt("page_size", false)
            .colBigInt("current_size", false)
            .colBigInt("maximum_size", false)
            .colBigInt("extension_size", false)
            .colSystemTimestamp("create_time", false)
            .colSystemTimestamp("open_time", false)
            .colSystemTimestamp("last_read_time", false)
            .colSystemTimestamp("last_write_time", false)
            .colSystemTimestamp("last_extension_time", false)
            .colBigInt("generation", false)
            .colBigInt("get_counter", false)
            .colBigInt("read_counter", false)
            .colBigInt("write_counter", false)
            .colBigInt("fetch_counter", false)
            .colBigInt("traverse_counter", false)
            .colBigInt("store_counter", false)
            .colBigInt("remove_counter", false);
        return builder.ais(false); 
    }
}
