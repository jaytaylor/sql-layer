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
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.google.inject.Inject;

public class StorageSchemaTablesServiceImpl implements Service<StorageSchemaTablesService>, StorageSchemaTablesService {

    private static final String SCHEMA_NAME = TableName.INFORMATION_SCHEMA;
    private static final String BASE_PERSITIT_JMX_PATH = "com.persistit:type=Persistit,class=";

    static final TableName STORAGE_CHECKPOINT_SUMMARY = new TableName (SCHEMA_NAME, "storage_checkpoint_summary");
    static final TableName STORAGE_IO_METER_SUMMARY = new TableName (SCHEMA_NAME, "storage_io_meter_summary");
    
    // Note: This doesn't use the treeService directly, but the internal processing requires
    // the underlying Persistit engine (which treeService controls) be up and running. 
    private final TreeService treeService;
    
    private final SchemaManager schemaManager;
    private final SessionService sessionService;
    private MBeanServer jmxServer;
    
    private final static Logger logger = LoggerFactory.getLogger(StorageSchemaTablesServiceImpl.class);
    
    @Inject
    public StorageSchemaTablesServiceImpl (SchemaManager schemaManager, TreeService treeService, SessionService sessionService) {
        this.schemaManager = schemaManager;
        this.treeService = treeService;
        this.sessionService = sessionService;
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
        Session session = sessionService.createSession();
        //STORAGE_CHECKPOINT_SUMMARY
        UserTable checkpointSummary = ais.getUserTable(STORAGE_CHECKPOINT_SUMMARY);
        assert checkpointSummary != null;
        schemaManager.registerMemoryInformationSchemaTable (session, checkpointSummary, new CheckpointSummaryFactory(STORAGE_CHECKPOINT_SUMMARY));

        //STORAGE_IO_METER_SUMMARY
        UserTable ioMeterSummary = ais.getUserTable(STORAGE_IO_METER_SUMMARY);
        assert ioMeterSummary != null;
        schemaManager.registerMemoryInformationSchemaTable(session, ioMeterSummary, new IoSummaryFactory (STORAGE_IO_METER_SUMMARY));
        
        session.close();
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
            logger.error (mbeanName.toString() + "#" + attributeName + " not found. This is an unexpected error");
        } catch (InstanceNotFoundException e) {
            logger.error(jmxServer.toString() + " JMX instance not found. This is an unexpected error");
        } catch (MBeanException e) {
            logger.error("Mbean retrival: " + mbeanName + " generated error: " + e.getMessage());
        } catch (ReflectionException e) {
            logger.error("Unexepcted reflection error: " + e.getMessage());
        }
        return value;
    }
    private class CheckpointSummaryFactory extends BasicFactoryBase {

        public CheckpointSummaryFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
       
        private class Scan implements GroupScan {
            final RowType rowType;
            int rowCounter = 0;
            private ObjectName mbeanName;
            
            public Scan (RowType rowType) {
                this.rowType = rowType;
                mbeanName = getBeanName("CheckpointManager");
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
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
        
        private class Scan implements GroupScan {
            final RowType rowType;
            int rowCounter = 0;
            private ObjectName mbeanName;
            
            public Scan (RowType rowType) {
                this.rowType = rowType;
                mbeanName = getBeanName("IOMeter");
            }
            
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesRow (rowType,
                        getJMXAttribute(mbeanName, "IORate"),
                        getJMXAttribute(mbeanName, "QuiescentIOthreshold"),
                        getJMXAttribute(mbeanName, "LogFile"),
                        ++rowCounter);
            }

            @Override
            public void close() {
            }
            
        }
        
    }

    static AkibanInformationSchema createTablesToRegister() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        
        builder.userTable(STORAGE_CHECKPOINT_SUMMARY)
            .colBigInt("checkpoint_interval", false);

        builder.userTable(STORAGE_IO_METER_SUMMARY)
            .colBigInt("io_rate", false)
            .colBigInt("quiescent_threshold", false)
            .colString("log_file", 1024);
        return builder.ais(false); 
    }
}
