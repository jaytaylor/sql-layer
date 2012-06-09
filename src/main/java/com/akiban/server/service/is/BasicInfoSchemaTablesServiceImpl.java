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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.memoryadapter.MemoryGroupCursor;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.GroupCursor;
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowValuesHolder;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.statistics.IndexStatistics;
import com.google.inject.Inject;

import java.util.Iterator;

public class BasicInfoSchemaTablesServiceImpl implements Service<BasicInfoSchemaTablesService>, BasicInfoSchemaTablesService {
    private final AisHolder aisHolder;
    private final SchemaManager schemaManager;
    private final SessionService sessionService;

    @Inject
    public BasicInfoSchemaTablesServiceImpl(AisHolder aisHolder, SchemaManager schemaManager, SessionService sessionService) {
        this.aisHolder = aisHolder;
        this.schemaManager = schemaManager;
        this.sessionService = sessionService;
    }

    @Override
    public BasicInfoSchemaTablesServiceImpl cast() {
        return this;
    }

    @Override
    public Class<BasicInfoSchemaTablesService> castClass() {
        return BasicInfoSchemaTablesService.class;
    }

    @Override
    public void start() {
        NewAISBuilder builder = AISBBasedBuilder.create(TableName.AKIBAN_INFORMATION_SCHEMA);
        builder.userTable("schemata")
                .colString("schema_name", 128, false)
                .colString("schema_owner", 128)
                .colString("default_character_set_name", 128)
                .colString("default_collation_name", 128);
                //.pk("schema_name")

        AkibanInformationSchema ais = builder.ais();

        Session session = sessionService.createSession();
        for(UserTable table : ais.getUserTables().values()) {
            Factory factory = new Factory(table);
            schemaManager.registerMemoryInformationSchemaTable(session, table, factory);
        }
    }

    @Override
    public void stop() {
        // Nothing
    }

    @Override
    public void crash() {
        // Nothing
    }

    private class Factory implements MemoryTableFactory {
        private final UserTable table;

        public Factory(UserTable table) {
            this.table = table;
        }

        @Override
        public TableName getName() {
            return table.getName();
        }

        @Override
        public UserTable getTableDefinition() {
            return table;
        }

        @Override
        public MemoryGroupCursor.TableScan getTableScan(final RowType rowType) {
            final Iterator<Schema> it = aisHolder.getAis().getSchemas().values().iterator();
            return new MemoryGroupCursor.TableScan() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Row next() {
                    Schema schema = it.next();
                    return new ValuesRow(rowType,
                                         schema.getName(), null, null, null, null);

                }
            };
        }

        @Override
        public Cursor getIndexCursor(Index index, Session session, IndexKeyRange keyRange, API.Ordering ordering, IndexScanSelector scanSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getSchemas().size();
        }

        @Override
        public IndexStatistics computeIndexStatistics(Session session, Index index) {
            throw new UnsupportedOperationException();
        }
    }
}
