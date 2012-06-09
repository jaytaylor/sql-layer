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
import com.akiban.ais.model.Column;
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
import com.akiban.qp.operator.IndexScanSelector;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.types.AkType;
import com.google.inject.Inject;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
        final TableName SCHEMATA = new TableName(TableName.AKIBAN_INFORMATION_SCHEMA, "schemata");
        final TableName TABLES = new TableName(TableName.AKIBAN_INFORMATION_SCHEMA, "tables");
        final TableName COLUMNS = new TableName(TableName.AKIBAN_INFORMATION_SCHEMA, "columns");

        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMATA)
                .colString("schema_name", 128, false)
                .colString("schema_owner", 128, true)
                .colString("default_character_set_name", 128, true)
                .colString("default_collation_name", 128, true);
                //.pk("schema_name")
        builder.userTable(TABLES)
                .colString("table_schema", 128, false)
                .colString("table_name", 128, false)
                .colString("group_name", 128, false)
                .colString("table_type", 128, false)
                .colLong("table_id", false)
                .colString("default_character_set_name", 128, false)
                .colString("default_collation_name", 128, false);
                //primary key (schema_name, table_name),
                //unique key (table_id),
                //foreign_key (schema_name) references schemata,
                //foreign key (schema_name, group_name) references groups);
        builder.userTable(COLUMNS)
                .colString("schema_name", 128, false)
                .colString("table_name", 128, false)
                .colString("column_name", 128, false)
                .colLong("position", false)
                .colString("type", 32, false)
                .colString("nullable", 3, false)
                .colLong("length", false)
                .colLong("precision", true)
                .colLong("scale", true)
                .colLong("prefix_size", true)
                .colBigInt("identity_start", true)
                .colString("character_set", 128, false)
                .colString("collation", 128, false);
                //primary key(schema_name, table_name, column_name),
                //foreign key(schema_name, table_name) references tables(schema_name, table_name),
                //foreign key (type) references types (type_name));

        Session session = sessionService.createSession();
        AkibanInformationSchema ais = builder.ais();
        {
            UserTable schemata = ais.getUserTable(SCHEMATA);
            schemaManager.registerMemoryInformationSchemaTable(session, schemata, new SchemaFactory(schemata));
        }
        {
            UserTable tables = ais.getUserTable(TABLES);
            schemaManager.registerMemoryInformationSchemaTable(session, tables, new TableFactory(tables));
        }
        {
            UserTable columns = ais.getUserTable(COLUMNS);
            schemaManager.registerMemoryInformationSchemaTable(session, columns, new ColumnsFactory(columns));
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

    private abstract class BasicFactoryBase implements MemoryTableFactory {
        private final UserTable sourceTable;

        public BasicFactoryBase(UserTable sourceTable) {
            this.sourceTable = sourceTable;
        }

        @Override
        public TableName getName() {
            return sourceTable.getName();
        }

        @Override
        public UserTable getTableDefinition() {
            return sourceTable;
        }

        @Override
        public Cursor getIndexCursor(Index index, Session session, IndexKeyRange keyRange, API.Ordering ordering, IndexScanSelector scanSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IndexStatistics computeIndexStatistics(Session session, Index index) {
            throw new UnsupportedOperationException();
        }
    }

    private class SchemaFactory extends BasicFactoryBase {
        public SchemaFactory(UserTable sourceTable) {
            super(sourceTable);
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
                                         schema.getName(),
                                         null,
                                         null,
                                         null,
                                         null,
                                         1 /*hidden pk*/);

                }
            };
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getSchemas().size();
        }
    }

    private class TableFactory extends BasicFactoryBase {
        public TableFactory(UserTable sourceTable) {
            super(sourceTable);
        }

        @Override
        public MemoryGroupCursor.TableScan getTableScan(final RowType rowType) {
            final Iterator<UserTable> it = aisHolder.getAis().getUserTables().values().iterator();
            return new MemoryGroupCursor.TableScan() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public Row next() {
                    UserTable table = it.next();
                    final String tableType = table.hasMemoryTableFactory() ? "DICTIONARY VIEW" : "TABLE";
                    return new ValuesRow(rowType,
                                         table.getName().getSchemaName(),
                                         table.getName().getTableName(),
                                         table.getGroup().getName(),
                                         tableType,
                                         table.getTableId(),
                                         table.getCharsetAndCollation().charset(),
                                         table.getCharsetAndCollation().collation(),
                                         1 /* hidden pk */);
                }
            };
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getUserTables().size();
        }
    }

    private class ColumnsFactory extends BasicFactoryBase {
        public ColumnsFactory(UserTable sourceTable) {
            super(sourceTable);
        }

        @Override
        public MemoryGroupCursor.TableScan getTableScan(final RowType rowType) {
            final Iterator[] it = {aisHolder.getAis().getUserTables().values().iterator(), null};
            return new MemoryGroupCursor.TableScan() {
                @Override
                public boolean hasNext() {
                    if(it[1] != null && it[1].hasNext()) {
                        return true;
                    }
                    return it[0].hasNext();
                }

                @Override
                public Row next() {
                    if((it[1] == null) || (!it[1].hasNext() && it[0].hasNext())) {
                        it[1] = ((UserTable)it[0].next()).getColumns().iterator();
                    }
                    Column column = (Column)it[1].next();

                    Integer scale = null;
                    Integer precision = null;
                    if(column.getType().akType() == AkType.DECIMAL) {
                        scale = column.getTypeParameter1().intValue();
                        precision = column.getTypeParameter2().intValue();
                    }

                    return new ValuesRow(rowType,
                                         column.getTable().getName().getSchemaName(),
                                         column.getTable().getName().getTableName(),
                                         column.getName(),
                                         column.getPosition(),
                                         column.getType().name(),
                                         column.getNullable() ? "YES" : "NO",
                                         column.getMaxStorageSize().intValue(),
                                         scale,
                                         precision,
                                         column.getPrefixSize(),
                                         column.getInitialAutoIncrementValue(),
                                         column.getCharsetAndCollation().charset(),
                                         column.getCharsetAndCollation().collation(),
                                         1 /* hidden pk */);
                }
            };
        }

        @Override
        public long rowCount() {
            long count = 0;
            for(UserTable table : aisHolder.getAis().getUserTables().values()) {
                count += table.getColumns().size(); // TODO: IncludingInternal()?
            }
            return count;
        }
    }
}
