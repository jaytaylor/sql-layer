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
    static final TableName STORAGE_CHECKPOINT_SUMMARY = new TableName (SCHEMA_NAME, "storage_checkpoint_summary");
    
    private final SchemaManager schemaManager;
    private final TreeService treeService;
    private final SessionService sessionService;

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
        AkibanInformationSchema ais = createTablesToRegister();
        Session session = sessionService.createSession();
        //STORAGE_CHECKPOINT_SUMMARY
        logger.debug("STORAGE_CHECKPOINT_SUMMARY");
        UserTable checkpointSummary = ais.getUserTable(STORAGE_CHECKPOINT_SUMMARY);
        assert checkpointSummary != null;
        schemaManager.registerMemoryInformationSchemaTable (session, checkpointSummary, new CheckpointSummaryFactory(checkpointSummary));
        session.close();
    }

    @Override
    public void stop() {
        // nothing
    }

    @Override
    public void crash() {
        // nothing
    }
    
    private class CheckpointSummaryFactory extends BasicFactoryBase {

        public CheckpointSummaryFactory(UserTable sourceTable) {
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
            
            public Scan (RowType rowType) {
                this.rowType = rowType;
            }
            @Override
            public Row next() {
                if (rowCounter != 0) {
                    return null;
                }
                return new ValuesRow (rowType,
                        new Long(treeService.getDb().getCheckpointIntervalNanos()),
                        ++rowCounter /* Hidden PK */); 
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
        
        return builder.ais(false); 
    }
}
