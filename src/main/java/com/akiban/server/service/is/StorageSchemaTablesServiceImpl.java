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
import com.google.inject.Inject;
import com.persistit.mxbeans.IOMeterMXBean;

public class StorageSchemaTablesServiceImpl implements Service<StorageSchemaTablesService>, StorageSchemaTablesService {

    private static final String SCHEMA_NAME = TableName.INFORMATION_SCHEMA;
    private static final String BASE_PERSITIT_JMX_PATH = "com.persistit:type=Persistit,class=";

    static final TableName STORAGE_ALERTS_SUMMARY = new TableName (SCHEMA_NAME, "storage_alert_summary");
    static final TableName STORAGE_CHECKPOINT_SUMMARY = new TableName (SCHEMA_NAME, "storage_checkpoint_summary");
    static final TableName STORAGE_IO_METER_SUMMARY = new TableName (SCHEMA_NAME, "storage_io_meter_summary");
    static final TableName STORAGE_IO_METERS = new TableName (SCHEMA_NAME, "storage_io_meters");

    
    public final static String[] OPERATION_NAME = { "None", 
        "Read page from Volume", 
        "Read page from Journal", 
        "Page copy from Journal to Volume", 
        "Write page from Journal", 
        "Transaction Start", 
        "Transaction Commit", 
        "Store Record", 
        "Delete Record or Range", 
        "Delete Tree", 
        "Other", 
        "Evict page from pool",
        "Flush journal", 
        "Get Page" };

    // Note: This doesn't use the treeService directly, but the internal processing requires
    // the underlying Persistit engine (which treeService controls) be up and running. 
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
        
        //STORAGE_CHECKPOINT_SUMMARY
        UserTable checkpointSummary = ais.getUserTable(STORAGE_CHECKPOINT_SUMMARY);
        assert checkpointSummary != null;
        schemaManager.registerMemoryInformationSchemaTable (checkpointSummary, new CheckpointSummaryFactory(STORAGE_CHECKPOINT_SUMMARY));

        //STORAGE_IO_METER_SUMMARY
        UserTable ioMeterSummary = ais.getUserTable(STORAGE_IO_METER_SUMMARY);
        assert ioMeterSummary != null;
        schemaManager.registerMemoryInformationSchemaTable(ioMeterSummary, new IoSummaryFactory (STORAGE_IO_METER_SUMMARY));
        
        UserTable ioMeters = ais.getUserTable(STORAGE_IO_METERS);
        assert ioMeters != null;
        schemaManager.registerMemoryInformationSchemaTable(ioMeters, new IOMetersFactory(STORAGE_IO_METERS));
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
                        OPERATION_NAME[rowCounter+1],
                        getJMXInvoke (mbeanName, "totalBytes", parameter.toArray()),
                        getJMXInvoke (mbeanName, "totalOperations", parameter.toArray()),
                        ++rowCounter);
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
        
        builder.userTable(STORAGE_CHECKPOINT_SUMMARY)
            .colBigInt("checkpoint_interval", false);

        builder.userTable(STORAGE_IO_METER_SUMMARY)
            .colBigInt("io_rate", false)
            .colBigInt("quiescent_threshold", false)
            .colString("log_file", 1024);
        
        builder.userTable(STORAGE_IO_METERS)
            .colString("operation", 64, false)
            .colBigInt("total_bytes", false)
            .colBigInt("operations", false);
        
        return builder.ais(false); 
    }
}
