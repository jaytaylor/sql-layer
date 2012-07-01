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

package com.akiban.server.service.is;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.memoryadapter.BasicFactoryBase;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.Service;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.google.inject.Inject;
import com.persistit.Management;
import com.persistit.Management.BufferPoolInfo;
import com.persistit.Management.JournalInfo;
import com.persistit.Management.TreeInfo;
import com.persistit.Management.VolumeInfo;
import com.persistit.mxbeans.IOMeterMXBean;

public class StorageSchemaTablesServiceImpl implements Service<StorageSchemaTablesService>, StorageSchemaTablesService {

    private static final String SCHEMA_NAME = TableName.INFORMATION_SCHEMA;
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
    
    private final SchemaManager schemaManager;
    private MBeanServer jmxServer;
    
    private final static Logger logger = LoggerFactory.getLogger(StorageSchemaTablesServiceImpl.class);
    
    @Inject
    public StorageSchemaTablesServiceImpl (SchemaManager schemaManager, TreeService treeService) {
        this.schemaManager = schemaManager;
        this.treeService = treeService;
    }

    @Override
    public StorageSchemaTablesService cast() {
        return this;
    }

    @Override
    public Class<StorageSchemaTablesService> castClass() {
        return StorageSchemaTablesService.class;
    }

    @Override
    public void start() {
        logger.debug("Starting Storage Schema Tables Service");
        jmxServer = ManagementFactory.getPlatformMBeanServer();

        AkibanInformationSchema ais = createTablesToRegister();
        
        //STORAGE_ALERTS_SUMMARY
        UserTable alertSummary = ais.getUserTable(STORAGE_ALERTS_SUMMARY);
        assert alertSummary  != null;
        schemaManager.registerMemoryInformationSchemaTable(alertSummary, new AlertSummaryFactory (STORAGE_ALERTS_SUMMARY));
        
        // STORAGE_BUFFER_POOLS
        UserTable bufferPools = ais.getUserTable(STORAGE_BUFFER_POOLS);
        assert bufferPools != null;
        schemaManager.registerMemoryInformationSchemaTable(bufferPools, new BufferPoolFactory (STORAGE_BUFFER_POOLS));
        
        //STORAGE_CHECKPOINT_SUMMARY
        UserTable checkpointSummary = ais.getUserTable(STORAGE_CHECKPOINT_SUMMARY);
        assert checkpointSummary != null;
        schemaManager.registerMemoryInformationSchemaTable (checkpointSummary, new CheckpointSummaryFactory(STORAGE_CHECKPOINT_SUMMARY));

        //STORAGE_CLEANUP_MANAGER_SUMMARY
        UserTable cleanupManagerSummary = ais.getUserTable(STORAGE_CLEANUP_MANAGER_SUMMARY);
        assert cleanupManagerSummary != null;
        schemaManager.registerMemoryInformationSchemaTable(cleanupManagerSummary, new CleanupSummaryFactory(STORAGE_CLEANUP_MANAGER_SUMMARY));
        
        //STORAGE_IO_METER_SUMMARY
        UserTable ioMeterSummary = ais.getUserTable(STORAGE_IO_METER_SUMMARY);
        assert ioMeterSummary != null;
        schemaManager.registerMemoryInformationSchemaTable(ioMeterSummary, new IoSummaryFactory (STORAGE_IO_METER_SUMMARY));

        // STORAGE_IO_METERS
        UserTable ioMeters = ais.getUserTable(STORAGE_IO_METERS);
        assert ioMeters != null;
        schemaManager.registerMemoryInformationSchemaTable(ioMeters, new IOMetersFactory(STORAGE_IO_METERS));
        
        //STORAGE_JOURNAL_MANAGER_SUMMARY
        UserTable journalManager = ais.getUserTable(STORAGE_JOURNAL_MANAGER_SUMMARY);
        assert journalManager != null;
        schemaManager.registerMemoryInformationSchemaTable(journalManager, new JournalManagerFactory (STORAGE_JOURNAL_MANAGER_SUMMARY));

        //STORAGE_MANAGEMENT_SUMMARY
        UserTable managementSummary = ais.getUserTable(STORAGE_MANAGEMENT_SUMMARY);
        assert managementSummary != null;
        schemaManager.registerMemoryInformationSchemaTable(managementSummary, new ManagementSummaryFactory(STORAGE_MANAGEMENT_SUMMARY));

        //STORAGE_TRANSACTION_SUMMARY
        UserTable transactionSummary = ais.getUserTable(STORAGE_TRANSACTION_SUMMARY);
        assert transactionSummary != null;
        schemaManager.registerMemoryInformationSchemaTable(transactionSummary, new TransactionSummaryFactory(STORAGE_TRANSACTION_SUMMARY));

        //STORAGE_TREES
        UserTable trees = ais.getUserTable(STORAGE_TREES);
        assert trees != null;
        schemaManager.registerMemoryInformationSchemaTable(trees, new TreesFactory(STORAGE_TREES));
        //STORAGE_VOLUMES
        UserTable volumes = ais.getUserTable(STORAGE_VOLUMES);
        assert volumes != null;
        schemaManager.registerMemoryInformationSchemaTable(volumes, new VolumesFactory(STORAGE_VOLUMES));
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
        Vector<String>signature = new Vector<String>(parameters.length);
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
    
    private abstract class Scan implements GroupScan {
        final RowType rowType;
        int rowCounter = 0;
        ObjectName mbeanName;
        
        public Scan (RowType rowType, String beanName) {
            this.rowType = rowType;
            mbeanName = getBeanName(beanName);
        }

        @Override
        public void close() {
        }
    }

    private class AlertSummaryFactory extends BasicFactoryBase {
        public AlertSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new AlertSummaryScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class AlertSummaryScan extends Scan {
            public AlertSummaryScan(RowType rowType) {
                super (rowType, "AlertMonitor");
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesRow (rowType,
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new BufferPoolScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            try {
                return treeService.getDb().getManagement().getBufferPoolInfoArray().length;
            } catch (RemoteException e) {
                return 0;
            }
        }
        
        private class BufferPoolScan extends Scan {
            BufferPoolInfo[] bufferPools = null;
            public BufferPoolScan (RowType rowType) {
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
                return new ValuesRow (rowType, 
                        bufferPools[rowCounter].getBufferSize(),
                        bufferPools[rowCounter].getBufferCount(),
                        bufferPools[rowCounter].getValidPageCount(),
                        bufferPools[rowCounter].getDirtyPageCount(),
                        bufferPools[rowCounter].getReaderClaimedPageCount(),
                        bufferPools[rowCounter].getWriterClaimedPageCount(),
                        bufferPools[rowCounter].getHitCount(),
                        bufferPools[rowCounter].getMissCount(),
                        bufferPools[rowCounter].getNewCount(),
                        bufferPools[rowCounter].getEvictCount(),
                        bufferPools[rowCounter].getWriteCount(),
                        bufferPools[rowCounter].getForcedCheckpointWriteCount(),
                        bufferPools[rowCounter].getForcedWriteCount(),
                        ++rowCounter);
            }
        }
    }
    
    private class CheckpointSummaryFactory extends BasicFactoryBase {

        public CheckpointSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new CheckpointScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
       
        private class CheckpointScan extends Scan{
            
            public CheckpointScan (RowType rowType) {
                super (rowType, "CheckpointManager");
             }
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesRow (rowType,
                            getJMXAttribute(mbeanName, "CheckpointInterval"),
                            ++rowCounter /* Hidden PK */);
            }

            @Override
            public void close() {
            }
        }
    }
    
    private class CleanupSummaryFactory extends BasicFactoryBase {
        public CleanupSummaryFactory (TableName sourceTable) {
            super (sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new CleanupScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class CleanupScan extends Scan {
            public CleanupScan (RowType rowType) {
                super (rowType, "CleanupManager");
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesRow (rowType,
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new IOScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class IOScan extends Scan{
         
            public IOScan (RowType rowType) {
                super (rowType, "IOMeter");
            }
            
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesRow (rowType,
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new IOMetersScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return IOMeterMXBean.OPERATIONS.length - 1;
        }
        
        private class IOMetersScan extends Scan {
            Vector<String> parameter;
            public IOMetersScan(RowType rowType) {
                super(rowType, "IOMeter");
                parameter = new Vector<String> (1);
                parameter.add(IOMeterMXBean.OPERATIONS[0]);
            }

            @Override
            public Row next() {
                if (rowCounter >= IOMeterMXBean.OPERATIONS.length - 1) {
                    return null;
                }
                parameter.set(0, IOMeterMXBean.OPERATIONS[rowCounter+1]);
                return new ValuesRow (rowType,
                        IOMeterMXBean.OPERATION_NAMES[rowCounter+1],
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new JournalManagerScan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class JournalManagerScan extends Scan {
            JournalInfo journal;
            public JournalManagerScan (RowType rowType) {
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
                               
                ValuesRow row = new ValuesRow (rowType,
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
                        journal.getLastValidCheckpointSystemTime(),
                        
                        getJMXAttribute (mbeanName, "PageListSize"),
                        getJMXAttribute(mbeanName, "FlushInterval"),
                        getJMXAttribute(mbeanName, "CopierInterval"),
                        getJMXInvoke(mbeanName, "urgency", params),
                        getJMXAttribute(mbeanName, "TotalCompletedCommits"),
                        getJMXAttribute(mbeanName, "CommitCompletionWaitTime"),
                        getJMXAttribute(mbeanName, "SlowIoAlertThreshold"),
                        boolResult((Boolean) getJMXAttribute(mbeanName, "RollbackPruningEnabled")),
                        getJMXAttribute(mbeanName, "JournalFilePath"),
                        getJMXAttribute(mbeanName, "JournalCreatedTime"),
                        ++rowCounter);
                
                ((FromObjectValueSource)row.eval(13)).setExplicitly(journal.getLastValidCheckpointSystemTime()/1000, AkType.TIMESTAMP);
                ((FromObjectValueSource)row.eval(23)).setExplicitly(((Long)getJMXAttribute(mbeanName, "JournalCreatedTime")).longValue()/1000, AkType.TIMESTAMP);
                return row;
            }
        }
    }

    //STORAGE_MANAGEMENT_SUMMARY
    private class ManagementSummaryFactory extends BasicFactoryBase {

        public ManagementSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new ManagementSummaryScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class ManagementSummaryScan extends Scan {

            Management db_manage;
            public ManagementSummaryScan(RowType rowType) {
                super(rowType, "Management");
                
                db_manage = treeService.getDb().getManagement();
            }
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                ValuesRow row;
                try {
                    row = new ValuesRow (rowType,
                            boolResult(db_manage.isInitialized()),
                            boolResult(db_manage.isUpdateSuspended()),
                            boolResult(db_manage.isShutdownSuspended()),
                            db_manage.getVersion(),
                            db_manage.getCopyright(),
                            db_manage.getStartTime(),
                            db_manage.getDefaultCommitPolicy(),
                            ++rowCounter);
                    ((FromObjectValueSource)row.eval(5)).setExplicitly(db_manage.getStartTime()/1000, AkType.TIMESTAMP);
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new TransactionSummaryScan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class TransactionSummaryScan extends Scan {

            public TransactionSummaryScan(RowType rowType) {
                super(rowType, "TransactionIndex");
            }

            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                Object activeTransactionFloor = getJMXAttribute(mbeanName, "ActiveTransactionFloor");
                Object activeTransactionCeiling = getJMXAttribute(mbeanName, "ActiveTransactionCeiling"); 
                ValuesRow row =  new ValuesRow(rowType,
                        activeTransactionFloor,
                        activeTransactionCeiling,
                        getJMXAttribute(mbeanName, "ActiveTransactionCount"),
                        getJMXAttribute(mbeanName, "CurrentCount"),
                        getJMXAttribute(mbeanName, "LongRunningCount"),
                        getJMXAttribute(mbeanName, "AbortedCount"),
                        getJMXAttribute(mbeanName, "FreeCount"),
                        getJMXAttribute(mbeanName, "DroppedCount"),
                        ++rowCounter);
                
                //((FromObjectValueSource)row.eval(0)).setExplicitly(activeTransactionFloor, AkType.TIMESTAMP);
                //((FromObjectValueSource)row.eval(1)).setExplicitly(activeTransactionCeiling, AkType.TIMESTAMP);
                return row;
            }
        }
    }
    
    //STORAGE_TREES
    private class TreesFactory extends BasicFactoryBase {

        public TreesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new TreeScan (getRowType(adapter));
        }

        @Override
        public long rowCount() {
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
        private class TreeScan extends Scan {

            VolumeInfo[] volumes;
            int volumeIndex = 0;
            TreeInfo[] trees = null;
            int treeIndex = 0;
            
            public TreeScan(RowType rowType) {
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
                ValuesRow row;
                if (volumes == null) {
                    return null;
                }
                if (volumeIndex >= volumes.length) {
                    return null;
                }
                if (trees == null) {
                    return null;
                }

                row = new ValuesRow (rowType,
                        volumes[volumeIndex].getName(),
                        trees[treeIndex].getName(),
                        trees[treeIndex].getStatus(),
                        trees[treeIndex].getDepth(),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new VolumesScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            try {
                return treeService.getDb().getManagement().getVolumeInfoArray().length;
            } catch (RemoteException e) {
                logger.error("Unable to get VolumeInfo array: " + e.getMessage());
                return 0;
            }
        }

        private class VolumesScan extends Scan {

            VolumeInfo[] volumes;
            public VolumesScan(RowType rowType) {
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
                
                ValuesRow row = new ValuesRow (rowType,
                        volumes[rowCounter].getName(),
                        volumes[rowCounter].getPath(),
                        boolResult(volumes[rowCounter].isTransient()),
                        volumes[rowCounter].getPageSize(),
                        volumes[rowCounter].getCurrentSize(),
                        volumes[rowCounter].getMaximumSize(),
                        volumes[rowCounter].getExtensionSize(),
                        volumes[rowCounter].getCreateTime().getTime(),
                        volumes[rowCounter].getOpenTime().getTime(),
                        volumes[rowCounter].getLastReadTime().getTime(),
                        volumes[rowCounter].getLastWriteTime().getTime(),
                        volumes[rowCounter].getLastExtensionTime().getTime(),
                        volumes[rowCounter].getGeneration(),
                        volumes[rowCounter].getGetCounter(),
                        volumes[rowCounter].getReadCounter(),
                        volumes[rowCounter].getWriteCounter(),
                        volumes[rowCounter].getFetchCounter(),
                        volumes[rowCounter].getTraverseCounter(),
                        volumes[rowCounter].getStoreCounter(),
                        volumes[rowCounter].getRemoveCounter(),
                        rowCounter);
                ((FromObjectValueSource)row.eval(7)).setExplicitly(volumes[rowCounter].getCreateTime().getTime()/1000, AkType.TIMESTAMP);
                ((FromObjectValueSource)row.eval(8)).setExplicitly(volumes[rowCounter].getOpenTime().getTime()/1000, AkType.TIMESTAMP);
                ((FromObjectValueSource)row.eval(9)).setExplicitly(volumes[rowCounter].getLastReadTime().getTime()/1000, AkType.TIMESTAMP);
                ((FromObjectValueSource)row.eval(10)).setExplicitly(volumes[rowCounter].getLastWriteTime().getTime()/1000, AkType.TIMESTAMP);
                ((FromObjectValueSource)row.eval(11)).setExplicitly(volumes[rowCounter].getLastExtensionTime().getTime()/1000, AkType.TIMESTAMP);
                ++rowCounter;
                return row;
            }
        }
    }
    static AkibanInformationSchema createTablesToRegister() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        
        builder.userTable(STORAGE_ALERTS_SUMMARY)
            .colString("alert_level", 64, false)
            .colBigInt("warn_log_interval", false)
            .colBigInt("error_log_interval", false)
            .colBigInt("history_length", false);
        
        
        builder.userTable(STORAGE_BUFFER_POOLS)
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
            
        builder.userTable(STORAGE_CHECKPOINT_SUMMARY)
            .colBigInt("checkpoint_interval", false);

        
        builder.userTable(STORAGE_CLEANUP_MANAGER_SUMMARY)
            .colBigInt ("accepted_count", false)
            .colBigInt ("refused_count", false)
            .colBigInt("performed_count", false)
            .colBigInt("error_count", false)
            .colBigInt("enqueued_count", false)
            .colBigInt("poll_interval", false)
            .colBigInt("min_prune_delay", false);

        builder.userTable(STORAGE_IO_METER_SUMMARY)
            .colBigInt("io_rate", false)
            .colBigInt("quiescent_threshold", false)
            .colString("log_file", 1024);
        
        builder.userTable(STORAGE_IO_METERS)
            .colString("operation", 64, false)
            .colBigInt("total_bytes", false)
            .colBigInt("operations", false);
        
        builder.userTable(STORAGE_JOURNAL_MANAGER_SUMMARY)
            .colBigInt("block_size", false)
            .colBigInt("base_address", false)
            .colBigInt("current_address", false)
            .colBigInt("page_map_size", false)
            .colBigInt("recovery_status", false)
            .colBigInt("journaled_page_count", false)
            .colBigInt("copied_page_count", false)
            .colBigInt("dropped_page_count", false)
            .colBigInt("read_page_count", false)
            .colString("append_only", 3, false)
            .colString("fast_copy", 3, false)
            .colString("copy_active", 3, false)
            .colString("flush_active", 3, false)
            .colTimestamp("checkpoint_time", false)
            
            .colBigInt("page_list_size", false)
            .colBigInt("flush_interval", false)
            .colBigInt("copier_interval", false)
            .colBigInt("copier_urgency", false)
            .colBigInt("total_commits", false)
            .colBigInt("commit_wait_time", false)
            .colBigInt("slow_alert_threshold", false)
            .colString("rollback_pruning_enabled", 3, false)

            .colString("file_path", 1024, false)
            .colTimestamp("create_time", false);

        
        builder.userTable(STORAGE_MANAGEMENT_SUMMARY)
            .colString("initialized", 3, false)
            .colString("update_suspended", 3, false)
            .colString("shutdown_suspended", 3, false)
            .colString("version", BasicFactoryBase.IDENT_MAX, false)
            .colString("copyright", BasicFactoryBase.IDENT_MAX, false)
            .colTimestamp("start_time", false)
            .colString("default_commit_policy", 64, false);
        
        builder.userTable(STORAGE_TRANSACTION_SUMMARY)
            .colBigInt("active_floor", false)
            .colBigInt("active_ceiling", false)
            .colBigInt("active_count", false)
            .colBigInt("current_count", false)
            .colBigInt("long_running_count", false)
            .colBigInt("aborted_count", false)
            .colBigInt("free_count",false)
            .colBigInt("dropped_count", false);

        builder.userTable(STORAGE_TREES)
            .colString("volume_name", BasicFactoryBase.IDENT_MAX, false)
            .colString("tree_name", BasicFactoryBase.IDENT_MAX, false)
            .colString("status", 64, false)
            .colBigInt("depth", false)
            .colBigInt("fetch_counter", false)
            .colBigInt("traverse_counter", false)
            .colBigInt("store_counter", false)
            .colBigInt("remove_counter", false);
            
        builder.userTable(STORAGE_VOLUMES)
            .colString("volume_name", BasicFactoryBase.IDENT_MAX, false)
            .colString("path", 1024, false)
            .colString("temporary", 3, false)
            .colBigInt("page_size", false)
            .colBigInt("current_size", false)
            .colBigInt("maximum_size", false)
            .colBigInt("extension_size", false)
            .colTimestamp("create_time", false)
            .colTimestamp("open_time", false)
            .colTimestamp("last_read_time", false)
            .colTimestamp("last_write_time", false)
            .colTimestamp("last_extension_time", false)
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
