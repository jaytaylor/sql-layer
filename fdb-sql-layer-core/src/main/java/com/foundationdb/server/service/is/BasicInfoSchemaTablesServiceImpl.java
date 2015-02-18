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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Constraint;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Parameter;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Schema;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.virtual.BasicFactoryBase;
import com.foundationdb.qp.virtual.VirtualAdapter;
import com.foundationdb.qp.virtual.VirtualGroupCursor;
import com.foundationdb.qp.virtual.VirtualGroupCursor.GroupScan;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.routines.ScriptEngineManagerProvider;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TBundleID;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TypeValidator;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.service.TypesRegistry;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;

import javax.script.ScriptEngineFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class BasicInfoSchemaTablesServiceImpl
    extends SchemaTablesService
    implements Service, BasicInfoSchemaTablesService {
    
    static final TableName SCHEMATA = new TableName(SCHEMA_NAME, "schemata");
    static final TableName TABLES = new TableName(SCHEMA_NAME, "tables");
    static final TableName COLUMNS = new TableName(SCHEMA_NAME, "columns");
    static final TableName TABLE_CONSTRAINTS = new TableName(SCHEMA_NAME, "table_constraints");
    static final TableName REFERENTIAL_CONSTRAINTS = new TableName(SCHEMA_NAME, "referential_constraints");
    static final TableName GROUPING_CONSTRAINTS = new TableName(SCHEMA_NAME, "grouping_constraints");
    static final TableName KEY_COLUMN_USAGE = new TableName(SCHEMA_NAME, "key_column_usage");
    static final TableName INDEXES = new TableName(SCHEMA_NAME, "indexes");
    static final TableName INDEX_COLUMNS = new TableName(SCHEMA_NAME, "index_columns");
    static final TableName SEQUENCES = new TableName(SCHEMA_NAME, "sequences");
    static final TableName VIEWS = new TableName(SCHEMA_NAME, "views");
    static final TableName VIEW_TABLE_USAGE = new TableName(SCHEMA_NAME, "view_table_usage");
    static final TableName VIEW_COLUMN_USAGE = new TableName(SCHEMA_NAME, "view_column_usage");
    static final TableName ROUTINES = new TableName(SCHEMA_NAME, "routines");
    static final TableName PARAMETERS = new TableName(SCHEMA_NAME, "parameters");
    static final TableName JARS = new TableName(SCHEMA_NAME, "jars");
    static final TableName ROUTINE_JAR_USAGE = new TableName(SCHEMA_NAME, "routine_jar_usage");
    static final TableName SCRIPT_ENGINES = new TableName(SCHEMA_NAME, "script_engines");
    static final TableName SCRIPT_ENGINE_NAMES = new TableName(SCHEMA_NAME, "script_engine_names");
    static final TableName TYPES = new TableName (SCHEMA_NAME, "types");
    static final TableName TYPE_BUNDLES = new TableName (SCHEMA_NAME, "type_bundles");
    static final TableName TYPE_ATTRIBUTES = new TableName (SCHEMA_NAME, "type_attributes");

    private final SecurityService securityService;
    private final ScriptEngineManagerProvider scriptEngineManagerProvider;
    private PostgresTypeMapper postgresTypeMapper = null;

    @Inject
    public BasicInfoSchemaTablesServiceImpl(SchemaManager schemaManager,
                                            SecurityService securityService,
                                            ScriptEngineManagerProvider scriptEngineManagerProvider) {
        super(schemaManager);
        this.securityService = securityService;
        this.scriptEngineManagerProvider = scriptEngineManagerProvider;
    }

    @Override
    public void start() {
        AkibanInformationSchema ais = createTablesToRegister(getTypesTranslator());
        attachFactories(ais);
    }

    @Override
    public void stop() {
        // Nothing
    }

    @Override
    public void crash() {
        // Nothing
    }

    @Override
    public void setPostgresTypeMapper(PostgresTypeMapper postgresTypeMapper) {
        this.postgresTypeMapper = postgresTypeMapper;
    }

    protected boolean isAccessible(Session session, String schemaName) {
        if ((session == null) || (securityService == null))
            return true;
        return securityService.isAccessible(session, schemaName);
    }

    protected boolean isAccessible(Session session, TableName name) {
        return isAccessible(session, name.getSchemaName());
    }

    protected TypesRegistry getTypesRegistry() {
        return schemaManager.getTypesRegistry();
    }

    protected TypesTranslator getTypesTranslator() {
        return schemaManager.getTypesTranslator();
    }

    private List<ScriptEngineFactory> getScriptEngineFactories() {
        return scriptEngineManagerProvider.getManager().getEngineFactories();
    }
    private AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    private class SchemataFactory extends BasicFactoryBase {
        public SchemataFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAIS(session).getSchemas().size();
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Schema> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                it = getAIS(session).getSchemas().values().iterator();
            }

            @Override
            public Row next() {
                while(it.hasNext()) {
                    Schema schema = it.next();
                    if(isAccessible(session, schema.getName())) {
                        return new ValuesHolderRow(rowType,
                                            null,        // catalog
                                             schema.getName(),
                                             null,              // owner
                                             null,null, null,   // default charset catalog/schema/name
                                             null,              // sql path
                                             null,null, null,   // default charset catalog/schema/name
                                             ++rowCounter /*hidden pk*/); 
                    }
                }
                return null;
           }
        }
    }

    private class TablesFactory extends BasicFactoryBase {
        public TablesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            AkibanInformationSchema ais = getAIS(session);
            return ais.getTables().size() + ais.getViews().size() ;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Table> tableIt;
            Iterator<View> viewIt = null;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.tableIt = getAIS(session).getTables().values().iterator();
            }

            @Override
            public Row next() {
                if(viewIt == null) {
                    while(tableIt.hasNext()) {
                        Table table = tableIt.next();
                        String tableType = (table.getName().inSystemSchema() ? "SYSTEM " : "") +
                                           (table.isVirtual() ? "VIEW" : "TABLE");
                        final Integer ordinal = table.isVirtual() ? null : table.getOrdinal();
                        final boolean isInsertable = !table.isVirtual();
                        if(isAccessible(session, table.getName())) {
                            return new ValuesHolderRow(rowType,
                                     null,       //catalog
                                     table.getName().getSchemaName(),
                                     table.getName().getTableName(),
                                     tableType,
                                     null,                  // self reference column
                                     null,                  // reference generation
                                     boolResult(isInsertable),
                                     boolResult(false),     // is types
                                     null,                  //commit action
                                     null,                  // charset catalog
                                     null,
                                     table.getDefaultedCharsetName().toLowerCase(),
                                     null,                  // collation catalog
                                     null,
                                     table.getDefaultedCollationName().toLowerCase(),
                                     table.getTableId(),
                                     ordinal,
                                     table.getGroup().getStorageNameString(),
                                     table.getGroup().getStorageDescription().getStorageFormat(),
                                     ++rowCounter /*hidden pk*/);
                        }
                    }
                    viewIt = getAIS(session).getViews().values().iterator();
                }
                while(viewIt.hasNext()) {
                    View view = viewIt.next();
                    if(isAccessible(session, view.getName())) {
                        return new ValuesHolderRow(rowType,
                                null, 
                                 view.getName().getSchemaName(),
                                 view.getName().getTableName(),
                                 "VIEW",
                                 null,      //self reference
                                 null,      //reference generation
                                 boolResult(false), // insertable
                                 boolResult(false), // is typed
                                 null,              // commit action
                                 null,null,null,    // charset catalog/schema/name
                                 null,null,null,    // collation catalog/schema/name
                                 null,              // tableId
                                 null,              // ordinal
                                 null,              // storage_name
                                 null,              // storage_format
                                 ++rowCounter /*hidden pk*/);
                    }
                }
                return null;
            }
        }
    }

    private class ColumnsFactory extends BasicFactoryBase {
        public ColumnsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            AkibanInformationSchema ais = getAIS(session);
            long count = 0;
            for(Table table : ais.getTables().values()) {
                count += table.getColumns().size();
            }
            for(View view : ais.getViews().values()) {
                count += view.getColumns().size();
            }
            return count;
        }
        
        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Table> tableIt;
            Iterator<View> viewIt = null;
            Iterator<Column> columnIt;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.tableIt = getAIS(session).getTables().values().iterator();
            }

            @Override
            public Row next() {
                getCols:
                while(columnIt == null || !columnIt.hasNext()) {
                    if(viewIt == null) {
                        while(tableIt.hasNext()) {
                            Table table = tableIt.next();
                            if(isAccessible(session, table.getName())) {
                                columnIt = table.getColumns().iterator();
                                continue getCols;
                            }
                        }
                        viewIt = getAIS(session).getViews().values().iterator();
                    }
                    while(viewIt.hasNext()) {
                        View view = viewIt.next();
                        if(isAccessible(session, view.getName())) {
                            columnIt = view.getColumns().iterator();
                            continue getCols;
                        }
                    }
                    return null;
                }

                Column column = columnIt.next();

                Long precision = null;
                Long scale = null;
                Long radix = null;
                String charset = null;
                String collation = null;
                if (column.getType().hasAttributes(DecimalAttribute.class)) {
                    precision = (long) column.getType().attribute(DecimalAttribute.PRECISION);
                    scale = (long) column.getType().attribute(DecimalAttribute.SCALE);
                    radix = 10L;
                }
                Long charMaxLength = null;
                Long charOctetLength = null;
                if (column.hasCharsetAndCollation()) {
                    charset = column.getCharsetName().toLowerCase();
                    collation = column.getCollationName().toLowerCase();
                }
                if (column.getType().hasAttributes(StringAttribute.class)) {
                    charMaxLength = (long) column.getType().attribute(StringAttribute.MAX_LENGTH);
                    charOctetLength = column.getMaxStorageSize() - column.getPrefixSize();
                }
                else if (column.getType().hasAttributes(TBinary.Attrs.class)) {
                    charMaxLength = charOctetLength = (long) column.getType().attribute(TBinary.Attrs.LENGTH);
                }
                String sequenceSchema = null;
                String sequenceName = null;
                String identityGeneration = null;
                long identityStart = 0, 
                        identityIncrement = 0, 
                        identityMin = 0, 
                        identityMax = 0;
                String identityCycle = null;
                if (column.getIdentityGenerator() != null) {
                    sequenceSchema = column.getIdentityGenerator().getSequenceName().getSchemaName();
                    sequenceName   = column.getIdentityGenerator().getSequenceName().getTableName();
                    identityGeneration = column.getDefaultIdentity() ? "BY DEFAULT" : "ALWAYS";
                    identityStart = column.getIdentityGenerator().getStartsWith();
                    identityIncrement = column.getIdentityGenerator().getIncrement();
                    identityMin = column.getIdentityGenerator().getMinValue();
                    identityMax = column.getIdentityGenerator().getMaxValue();
                    identityCycle = boolResult(column.getIdentityGenerator().isCycle()); 
                }
                String defaultString = null;
                if (column.getDefaultValue() != null) {
                    defaultString = column.getDefaultValue();
                }
                else if (column.getDefaultFunction() != null) {
                    defaultString = column.getDefaultFunction() + "()";
                }
                
                return new ValuesHolderRow(rowType,
                                    null,
                                     column.getColumnar().getName().getSchemaName(),
                                     column.getColumnar().getName().getTableName(),
                                     column.getName(),
                                     column.getPosition().longValue(),
                                     defaultString,
                                     boolResult(column.getNullable()),
                                     column.getTypeName(),
                                     charMaxLength,
                                     charOctetLength,
                                     precision,
                                     radix,
                                     scale,
                                     null,          // charset catalog
                                     null,          // charset schema
                                     charset,
                                     null,              // collation catalog
                                     null,              //collation schema
                                     collation,
                                     null,null,null,    // domain catalog/schema/name
                                     null,null,null,    // udt catalog/schema/name
                                     null,null,null,    // scope catalog/schema/name
                                     null,              //maximum cardinality
                                     boolResult(false), // is self referencing
                                     boolResult(identityGeneration != null),
                                     identityGeneration,
                                     identityGeneration != null ? identityStart : null,
                                     identityGeneration != null ? identityIncrement : null,
                                     identityGeneration != null ? identityMin : null,
                                     identityGeneration != null ? identityMax : null,
                                     identityCycle,
                                     boolResult(false), // is generated
                                     null,              // generation expression
                                     boolResult(true),  // is Updatable
                                     null,              // sequence catalog
                                     sequenceSchema,
                                     sequenceName,
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class TableConstraintsFactory extends BasicFactoryBase {
        public TableConstraintsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private TableConstraintsIteration newIteration(Session session,
                                                       AkibanInformationSchema ais) {
            return new TableConstraintsIteration(session, ais.getTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType( group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            AkibanInformationSchema ais = getAIS(session);
            long count = 0;
            TableConstraintsIteration it = newIteration(null, ais);
            while(it.next()) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final TableConstraintsIteration it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.it = newIteration(session, getAIS(session));
            }

            @Override
            public Row next() {
                if(!it.next()) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        null,   //constraint catalog
                         it.getTable().getName().getSchemaName(),
                         it.getName(),
                         null,          // table_catalog
                         it.getTable().getName().getSchemaName(),
                         it.getTable().getName().getTableName(),
                         it.getType(),
                         boolResult(it.isDeferrable()),
                         boolResult(it.isInitiallyDeferred()),
                         boolResult(true),  // enforced
                         ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class ReferentialConstraintsFactory extends BasicFactoryBase {
        public ReferentialConstraintsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private TableConstraintsIteration newIteration(Session session,
                                                       AkibanInformationSchema ais) {
            return new TableConstraintsIteration(session, ais.getTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            AkibanInformationSchema ais = getAIS(session);
            long count = 0;
            TableConstraintsIteration it = newIteration(null, ais);
            while(it.next()) {
                if (it.isForeignKey()) {
                    ++count;
                }
            }
            return count;
        }

        private class Scan extends BaseScan {
            final TableConstraintsIteration it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.it = newIteration(session, getAIS(session));
            }

            @Override
            public Row next() {
                do {
                    if(!it.next()) {
                        return null;
                    }
                } while (!it.isForeignKey());
                ForeignKey fk = (ForeignKey)it.getConstraint();
                return new ValuesHolderRow(rowType,
                        null,   //constraint catalog
                         fk.getConstraintName().getSchemaName(),
                         fk.getConstraintName().getTableName(),
                         null,          //unique_constraint catalog
                         fk.getReferencedIndex().getConstraintName().getSchemaName(),
                         fk.getReferencedIndex().getConstraintName().getTableName(),
                         "SIMPLE",
                         fk.getUpdateAction().toSQL(),
                         fk.getDeleteAction().toSQL(),
                         ++rowCounter /*hidden pk*/);
            }
        }
    }

    private static class RootPathTable {
        final Table root;
        final String path;
        final Table table;

        public RootPathTable(Table root, String path, Table table) {
            this.root = root;
            this.path = path;
            this.table = table;
        }

        @Override
        public String toString() {
            return root.getName() + ", " + table.getName() + ", " + path;
        }
    }

    private static class TableIDComparator implements Comparator<Table> {
        @Override
        public int compare(Table o1, Table o2) {
            return o1.getTableId().compareTo(o2.getTableId());
        }
    }
    private static final TableIDComparator TABLE_ID_COMPARATOR = new TableIDComparator();

    private static void addBranchToList(List<RootPathTable> list, StringBuilder builder, Table root, Table branch) {
        if(builder.length() != 0) {
            builder.append('/');
        }
        builder.append(branch.getName().getSchemaName());
        builder.append('.');
        builder.append(branch.getName().getTableName());
        list.add(new RootPathTable(root, builder.toString(), branch));

        // For tables at the same depth, comparing table IDs is currently synonymous with ordinals
        List<Table> children = new ArrayList<>();
        for(Join join : branch.getChildJoins()) {
            children.add(join.getChild());
        }
        Collections.sort(children, TABLE_ID_COMPARATOR);

        for(Table child : children) {
            int saveLen = builder.length();
            addBranchToList(list, builder, root, child);
            builder.setLength(saveLen);
        }
    }

    private class GroupingConstraintsFactory extends BasicFactoryBase {
        public GroupingConstraintsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAIS(session).getTables().size();
        }

        private class Scan extends BaseScan {
            final List<RootPathTable> rootPathTables;
            final Iterator<RootPathTable> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                AkibanInformationSchema ais = getAIS(session);

                // Desired output: groups together, ordered by branch (ordinal), then ordered by depth
                // Highest level sorting will be by schema.root, which seems as good as any
                rootPathTables = new ArrayList<>();
                Collection<Table> allTables = new ArrayList<>();
                for(Table table : ais.getTables().values()) {
                    if(isAccessible(session, table.getName())) {
                        allTables.add(table);
                    }
                }
                StringBuilder builder = new StringBuilder();
                for(Table table : allTables) {
                    if(table.isRoot()) {
                        addBranchToList(rootPathTables, builder, table, table);
                        builder.setLength(0);
                    }
                }
                assert rootPathTables.size() == allTables.size() : "Didn't collect all tables";
                it = rootPathTables.iterator();
            }

            @Override
            public Row next() {
                if (!it.hasNext()) {
                    return null;
                }
                
                RootPathTable rpt = it.next();
                Table table = rpt.table;
                String constraintName = null;
                String uniqueSchema = null;
                String uniqueConstraint = null;

                Join join = table.getParentJoin();
                if (table.getParentJoin() != null) {
                    constraintName = join.getConstraintName().getTableName();
                    uniqueSchema = join.getParent().getName().getSchemaName();
                    uniqueConstraint = join.getParent().getPrimaryKey().getIndex().getConstraintName().getTableName();
                }
                
                return new ValuesHolderRow(rowType,
                                    null,                               //root table catalog
                                     rpt.root.getName().getSchemaName(),// root_table_schema
                                     rpt.root.getName().getTableName(), // root_table_name
                                     null,                              //constraint catalog
                                     table.getName().getSchemaName(),   // constraint_schema
                                     table.getName().getTableName(),    // constraint_table_name
                                     rpt.path,                          // path
                                     table.getDepth().longValue(),      // depth
                                     constraintName,                    // constraint_name
                                     null,                              // unique_catalog
                                     uniqueSchema,                      // unique_schema
                                     uniqueConstraint,                  // unique_constraint_name
                                     ++rowCounter);
            }
        }
    }

    private class KeyColumnUsageFactory extends BasicFactoryBase {
        public KeyColumnUsageFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private TableConstraintsIteration newIteration(Session session,
                                                       AkibanInformationSchema ais) {
            return new TableConstraintsIteration(session, ais.getTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            int count = 0;
            TableConstraintsIteration it = newIteration(null, getAIS(session));
            while(it.next()) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final TableConstraintsIteration it;
            Iterator<IndexColumn> indexColIt;
            Iterator<JoinColumn> joinColIt;
            Iterator<JoinColumn> fkColIt;
            String colName;
            int colPos;
            Long posInUnique;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.it = newIteration(session, getAIS(session));
            }

            // Find position in parents PK
            private Integer findPosInIndex(Column column, Index index) {
                // Find position in the parents pk
                for(IndexColumn indexCol : index.getKeyColumns()) {
                    if(column == indexCol.getColumn()) {
                        return indexCol.getPosition();
                    }
                }
                return null;
            }

            public boolean advance() {
                if(joinColIt != null && joinColIt.hasNext()) {
                    JoinColumn joinColumn = joinColIt.next();
                    colName = joinColumn.getChild().getName();
                    posInUnique = findPosInIndex(joinColumn.getParent(), joinColumn.getParent().getTable().getPrimaryKey().getIndex()).longValue();
                } else if(indexColIt != null && indexColIt.hasNext()) {
                    IndexColumn indexColumn = indexColIt.next();
                    colName = indexColumn.getColumn().getName();
                    posInUnique = null;
                } else if(fkColIt != null && fkColIt.hasNext()) {
                    ForeignKey fk = (ForeignKey)it.getConstraint();
                    colName = fkColIt.next().getChild().getName();
                    posInUnique = findPosInIndex(fk.getReferencedColumns().get(colPos+1), fk.getReferencedIndex()).longValue();
                } else if(it.next()) {
                    joinColIt = null;
                    indexColIt = null;
                    fkColIt = null;
                    if(it.isGrouping()) {
                        joinColIt = ((Join)it.getConstraint()).getJoinColumns().iterator();
                    } else if(it.isIndex()) {
                        indexColIt = ((Index)it.getConstraint()).getKeyColumns().iterator();
                    } else if(it.isForeignKey()) {
                        fkColIt = ((ForeignKey)it.getConstraint()).getJoinColumns().iterator();
                    }
                    colPos = -1;
                    return advance();
                } else {
                    return false;
                }
                ++colPos;
                return true;
            }

            @Override
            public Row next() {
                if(!advance()) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        null,       // constraint catalog, 
                        it.getTable().getName().getSchemaName(),
                        it.getName(),
                        null,       // table catalog
                        it.getTable().getName().getSchemaName(),
                        it.getTable().getName().getTableName(),
                        colName,
                        colPos,
                        posInUnique,
                        ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class IndexesFactory extends BasicFactoryBase {
        public IndexesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private IndexIteration newIteration(Session session,
                                            AkibanInformationSchema ais) {
            return new IndexIteration(session, ais.getTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            IndexIteration it = newIteration(null, getAIS(session));
            while(it.next() != null) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final IndexIteration indexIt;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.indexIt = newIteration(session, getAIS(session));
            }

            @Override
            public Row next() {
                Index index = indexIt.next();
                if(index == null) {
                    return null;
                }
                final String indexType;
                if(index.isPrimaryKey()) {
                    indexType = "PRIMARY";
                } else if(index.isUnique()) {
                    indexType = "UNIQUE";
                } else {
                    indexType = "INDEX";
                }
                return new ValuesHolderRow(rowType,
                        null, 
                        indexIt.getTable().getName().getSchemaName(),
                        indexIt.getTable().getName().getTableName(),
                        index.getIndexName().getName(),
                        null, 
                        index.getConstraintName() == null ? null : index.getConstraintName().getSchemaName(),
                        index.getConstraintName() == null ? null : index.getConstraintName().getTableName(),
                        index.getIndexId(),
                        index.getStorageNameString(),
                        index.getStorageDescription().getStorageFormat(),
                        indexType,
                        boolResult(index.isUnique()),
                        index.isGroupIndex() ? index.getJoinType().name() : null,
                        (index.getIndexMethod() == Index.IndexMethod.NORMAL) ? null : index.getIndexMethod().name(),
                        ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class IndexColumnsFactory extends BasicFactoryBase {
        public IndexColumnsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private IndexIteration newIteration(Session session,
                                            AkibanInformationSchema ais) {
            return new IndexIteration(session, ais.getTables().values().iterator());
        }

        @Override
        public VirtualGroupCursor.GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            IndexIteration indexIt = newIteration(null, getAIS(session));
            long count = 0;
            Index index;
            while((index = indexIt.next()) != null) {
                count += index.getKeyColumns().size();
            }
            return count;
        }

        private class Scan extends BaseScan {
            final IndexIteration indexIt;
            Iterator<IndexColumn> indexColumnIt;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.indexIt = newIteration(session, getAIS(session));
            }

            private IndexColumn advance() {
                while(indexColumnIt == null || !indexColumnIt.hasNext()) {
                    Index index = indexIt.next();
                    if(index == null) {
                        return null;
                    }
                    indexColumnIt = index.getKeyColumns().iterator(); // Always at least 1
                }
                return indexColumnIt.next();
            }

            @Override
            public Row next() {
                IndexColumn indexColumn = advance();
                if(indexColumn == null) {
                    return null;
                }
                return new ValuesHolderRow(rowType,
                        null,
                         indexIt.getTable().getName().getSchemaName(),
                         indexIt.getTable().getName().getTableName(),
                         indexColumn.getIndex().getIndexName().getName(),
                         null,
                         indexColumn.getColumn().getTable().getName().getSchemaName(),
                         indexColumn.getColumn().getTable().getName().getTableName(),
                         indexColumn.getColumn().getName(),
                         indexColumn.getPosition().longValue(),
                         boolResult(indexColumn.isAscending()),
                         ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class SequencesFactory extends BasicFactoryBase {
        public SequencesFactory (TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType( group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAIS(session).getSequences().size();
        }
        
        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Sequence> it;
            final static String datatype = "bigint";
            
            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.it =  getAIS(session).getSequences().values().iterator();
            }

            @Override
            public Row next() {
                while(it.hasNext()) {
                    Sequence sequence = it.next();
                    if(isAccessible(session, sequence.getSequenceName()) && 
                            !sequence.isInternalSequence()) {
                        return new ValuesHolderRow(rowType,
                                             null,      //sequence catalog
                                             sequence.getSequenceName().getSchemaName(),
                                             sequence.getSequenceName().getTableName(),
                                             datatype,
                                             sequence.getStartsWith(),
                                             sequence.getMinValue(),
                                             sequence.getMaxValue(),
                                             sequence.getIncrement(),
                                             boolResult(sequence.isCycle()),
                                             sequence.getStorageNameString(),
                                             ++rowCounter);
                    }
                }
                return null;
            }
        }
    }
    private class ViewsFactory extends BasicFactoryBase {
        public ViewsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType( group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAIS(session).getViews().size();
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<View> it;
            final static String checkOption = "NONE";

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                it = getAIS(session).getViews().values().iterator();
            }

            @Override
            public Row next() {
                while(it.hasNext()) {
                    View view = it.next();
                    if(isAccessible(session, view.getName())) {
                        return new ValuesHolderRow(rowType,
                                            null,       // view catalog
                                             view.getName().getSchemaName(),
                                             view.getName().getTableName(),
                                             view.getDefinition(),
                                             checkOption,
                                             boolResult(false),
                                             boolResult(false),
                                             boolResult(false),
                                             boolResult(false),
                                             boolResult(false),
                                             ++rowCounter /*hidden pk*/);
                    }
                } 
                return null;
            }
        }
    }

    private class ViewTableUsageFactory extends BasicFactoryBase {
        public ViewTableUsageFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            for (View view : getAIS(session).getViews().values()) {
                count += view.getTableReferences().size();
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<View> viewIt;
            View view;
            Iterator<TableName> tableIt = null;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.viewIt =  getAIS(session).getViews().values().iterator();
            }

            @Override
            public Row next() {
                getTables:
                while((tableIt == null) || !tableIt.hasNext()) {
                    while(viewIt.hasNext()) {
                        view = viewIt.next();
                        if(isAccessible(session, view.getName())) {
                            tableIt = view.getTableReferences().iterator();
                            continue getTables;
                        }
                    }
                    return null;
                }
                TableName table = tableIt.next();
                return new ValuesHolderRow(rowType,
                                    null,
                                     view.getName().getSchemaName(),
                                     view.getName().getTableName(),
                                    null,
                                     table.getSchemaName(),
                                     table.getTableName(),
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class ViewColumnUsageFactory extends BasicFactoryBase {
        public ViewColumnUsageFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            for (View view : getAIS(session).getViews().values()) {
                for (Collection<String> columns : view.getTableColumnReferences().values()) {
                    count += columns.size();
                }
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<View> viewIt;
            View view;
            Iterator<Map.Entry<TableName,Collection<String>>> tableIt = null;
            TableName table;
            Iterator<String> columnIt = null;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.viewIt = getAIS(session).getViews().values().iterator();
            }

            @Override
            public Row next() {
                while((columnIt == null) || !columnIt.hasNext()) {
                    getTables:
                    while((tableIt == null) || !tableIt.hasNext()) {
                        while(viewIt.hasNext()) {
                            view = viewIt.next();
                            if(isAccessible(session, view.getName())) {
                                tableIt = view.getTableColumnReferences().entrySet().iterator();                            
                                continue getTables;
                            }
                        }
                        return null;
                    }
                    Map.Entry<TableName,Collection<String>> entry = tableIt.next();
                    table = entry.getKey();
                    columnIt = entry.getValue().iterator();
                }
                String column = columnIt.next();
                return new ValuesHolderRow(rowType,
                                    null,
                                     view.getName().getSchemaName(),
                                     view.getName().getTableName(),
                                    null,
                                     table.getSchemaName(),
                                     table.getTableName(),
                                     column,
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }
    
    private class TableConstraintsIteration {
        private final Session session;
        private final Iterator<Table> tableIt;
        // For PRIMARY and UNIQUE
        private Iterator<? extends Index> indexIt;
        // For FOREIGN KEY
        private Iterator<ForeignKey> foreignKeyIt;
        private Table curTable;
        private Constraint curConstraint;
        private String type;
        private boolean isDeferrable = false;
        private boolean isInitiallyDeferred = false;

        public TableConstraintsIteration(Session session, Iterator<Table> tableIt) {
            this.session = session;
            this.tableIt = tableIt;
        }

        public boolean next() {
            while(curTable != null || tableIt.hasNext()) {
                if(curTable == null) {
                    curTable = tableIt.next();
                    if (!isAccessible(session, curTable.getName())) {
                        curTable = null;
                        continue;
                    }
                    curConstraint = curTable.getParentJoin();
                    if(curConstraint != null) {
                        type = "GROUPING";
                        return true;
                    }
                }
                if(indexIt == null) {
                    indexIt = curTable.getIndexes().iterator();
                }
                while(indexIt.hasNext()) {
                    Index curIndex = indexIt.next();
                    if(curIndex.isUnique()) {
                        curConstraint = curIndex;
                        type = curIndex.isPrimaryKey() ? "PRIMARY KEY" : "UNIQUE";
                        isDeferrable = false;
                        isInitiallyDeferred = false;
                        return true;
                    }
                }
                if(foreignKeyIt == null) {
                    foreignKeyIt = curTable.getReferencingForeignKeys().iterator();
                }
                if(foreignKeyIt.hasNext()) {
                    ForeignKey fk = foreignKeyIt.next();
                    curConstraint = fk;
                    type = "FOREIGN KEY";
                    isDeferrable = fk.isDeferrable();
                    isInitiallyDeferred = fk.isInitiallyDeferred();
                    return true;
                }
                curConstraint = null;
                foreignKeyIt = null;
                indexIt = null;
                curTable = null;
            }
            return false;
        }

        public String getName() {
            return curConstraint.getConstraintName().getTableName();
        }

        public String getType() {
            return type;
        }

        public Table getTable() {
            return curTable;
        }

        public Constraint getConstraint() {
            return curConstraint;
        }

        public boolean isIndex() {
            return (curConstraint instanceof Index);
        }

        public boolean isGrouping() {
            return (curConstraint instanceof Join);
        }

        public boolean isDeferrable() {
            return isDeferrable;
        }
        
        public boolean isInitiallyDeferred() {
            return isInitiallyDeferred;
        }
        
        public boolean isForeignKey() {
            return (curConstraint instanceof ForeignKey);
        }
    }

    private class IndexIteration {
        private final Session session;
        private final Iterator<Table> tableIt;
        Iterator<TableIndex> tableIndexIt;
        Iterator<GroupIndex> groupIndexIt;
        Iterator<FullTextIndex> textIndexIt;
        Table curTable;

        public IndexIteration(Session session,
                              Iterator<Table> tableIt) {
            this.session = session;
            this.tableIt = tableIt;
        }

        public Index next() {
            getIndexes:
            while(tableIndexIt == null || !tableIndexIt.hasNext()) {
                while(groupIndexIt != null && groupIndexIt.hasNext()) {
                    GroupIndex index = groupIndexIt.next();
                    if(index.leafMostTable() == curTable) {
                        return index;
                    }
                }
                while(textIndexIt != null && textIndexIt.hasNext()) {
                    return textIndexIt.next();
                }
                while(tableIt.hasNext()) {
                    curTable = tableIt.next();
                    if(isAccessible(session, curTable.getName())) {
                        tableIndexIt = curTable.getIndexes().iterator();
                        groupIndexIt = curTable.getGroup().getIndexes().iterator();
                        textIndexIt = curTable.getOwnFullTextIndexes().iterator();
                        continue getIndexes;
                    }
                } 
                return null;
            }
            return tableIndexIt.next();
        }

        public Table getTable() {
            return curTable;
        }
    }

    private class RoutinesFactory extends BasicFactoryBase {
        public RoutinesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAIS(session).getRoutines().size() ;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Routine> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.it = getAIS(session).getRoutines().values().iterator();
            }

            @Override
            public Row next() {
                while(it.hasNext()) {
                    Routine routine = it.next();
                    if(isAccessible(session, routine.getName())) {
                        String routineType = (routine.getName().inSystemSchema() ? "SYSTEM " : "") +
                                             (routine.isProcedure() ? "PROCEDURE" : "FUNCTION");
                        return new ValuesHolderRow(rowType,
                                            null,
                                             routine.getName().getSchemaName(),
                                             routine.getName().getTableName(),
                                            null, 
                                             routine.getName().getSchemaName(),
                                             routine.getName().getTableName(),
                                             routineType,
                                             null, null, null,                  // module catalog/schema/name
                                             null, null, null,                  // udt catalog/schema/name
                                             routine.getLanguage().equals("SQL") ? "SQL" : "EXTERNAL", // body
                                             routine.getDefinition(),                           // definition
                                             routine.getExternalName(),                         //external name
                                             routine.getLanguage(),                             //language
                                             routine.getCallingConvention().name(), //parameter_style
                                             boolResult(false),                     //is deterministic
                                             (routine.getSQLAllowed() == null) ? null : routine.getSQLAllowed().name().replace('_', ' '),
                                             routine.isProcedure() ? null : boolResult(!routine.isCalledOnNullInput()),
                                             null, //sql path
                                             boolResult(true),      // schema level routine
                                             (long)(routine.getDynamicResultSets()),
                                             null, null, // defined cast, implicit invoke
                                             null, //security type
                                             null, //as locator
                                             boolResult(false), //is udt dependent
                                             null, //created timestamp
                                             null, //last updated timestamp
                                             null, //new savepoint level
                                             ++rowCounter /*hidden pk*/);
                    }
                }
                return null;
            }
        }
    }

    private class ParametersFactory extends BasicFactoryBase {
        public ParametersFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            for (Routine routine : getAIS(session).getRoutines().values()) {
                if (routine.getReturnValue() != null) {
                    count++;
                }
                count += routine.getParameters().size();
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Routine> routinesIt;
            Iterator<Parameter> paramsIt;
            long ordinal;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.routinesIt = getAIS(session).getRoutines().values().iterator();
            }

            @Override
            public Row next() {
                Parameter param;
                while (true) {
                    if (paramsIt != null) {
                        if (paramsIt.hasNext()) {
                            param = paramsIt.next();
                            ordinal++;
                            break;
                        }
                    }
                    if (!routinesIt.hasNext())
                        return null;
                    Routine routine = routinesIt.next();
                    if (!isAccessible(session, routine.getName()))
                        continue;
                    paramsIt = routine.getParameters().iterator();
                    ordinal = 0;
                    param = routine.getReturnValue();
                    if (param != null) {
                        ordinal++;
                        break;
                    }
                }
                Long length = null;
                Long precision = null;
                Long scale = null;
                Long radix = null;

                if (param.getType().hasAttributes(StringAttribute.class))
                {
                    length = (long)param.getType().attribute(StringAttribute.MAX_LENGTH);
                } else if (param.getType().hasAttributes(DecimalAttribute.class)) {
                    precision = (long)param.getType().attribute(DecimalAttribute.PRECISION);
                    scale = (long)param.getType().attribute(DecimalAttribute.SCALE);
                    radix = 10L;
                }
                return new ValuesHolderRow(rowType,
                                    null, //Routine catalog
                                     param.getRoutine().getName().getSchemaName(),
                                     param.getRoutine().getName().getTableName(),
                                     param.getName(),
                                     ordinal,
                                     param.getTypeName(),
                                     length, 
                                     precision,
                                     radix,
                                     scale,
                                     (param.getDirection() == Parameter.Direction.RETURN) ? "OUT" : param.getDirection().name(),
                                     boolResult(param.getDirection() == Parameter.Direction.RETURN),
                                     null, //parameter default
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class JarsFactory extends BasicFactoryBase {
        public JarsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            return getAIS(session).getSQLJJars().size() ;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<SQLJJar> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.it = getAIS(session).getSQLJJars().values().iterator();
            }

            @Override
            public Row next() {
                while(it.hasNext()) {
                    SQLJJar jar = it.next();
                    if(isAccessible(session, jar.getName())) {
                        return new ValuesHolderRow(rowType,
                                            null, //jar catalog
                                             jar.getName().getSchemaName(),
                                             jar.getName().getTableName(),
                                             jar.getURL().toExternalForm(),
                                             ++rowCounter /*hidden pk*/);
                    }
                }
                return null;
            }
        }
    }

    private class RoutineJarUsageFactory extends BasicFactoryBase {
        public RoutineJarUsageFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(adapter.getSession(), getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session session) {
            long count = 0;
            for (Routine routine : getAIS(session).getRoutines().values()) {
                if (routine.getSQLJJar() != null) {
                    count++;
                }
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Routine> it;

            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.it = getAIS(session).getRoutines().values().iterator();
            }

            @Override
            public Row next() {
                while (it.hasNext()) {
                    Routine routine = it.next();
                    if (!isAccessible(session, routine.getName()))
                        continue;
                    SQLJJar jar = routine.getSQLJJar();
                    if (jar != null) {
                        return new ValuesHolderRow(rowType,
                                            null,       // routine catalog
                                             routine.getName().getSchemaName(),
                                             routine.getName().getTableName(),
                                            null,       // jar catalog
                                             jar.getName().getSchemaName(),
                                             jar.getName().getTableName(),
                                             ++rowCounter /*hidden pk*/);
                    }
                }
                return null;
            }
        }
    }

    private class ScriptEnginesISFactory extends BasicFactoryBase {
        public ScriptEnginesISFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public long rowCount(Session session) {
            return getScriptEngineFactories().size();
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(getScriptEngineFactories().listIterator(), getRowType(group.getAIS()));
        }

        private class Scan extends BaseScan {
            final ListIterator<ScriptEngineFactory> it;

            public Scan(ListIterator<ScriptEngineFactory> it, RowType rowType) {
                super(rowType);
                this.it = it;
            }

            @Override
            public Row next() {
                if (!it.hasNext())
                    return null;
                ScriptEngineFactory factory = it.next();
                return new ValuesHolderRow(
                        rowType,
                        it.nextIndex(), // use nextIndex so that the IDs are 1-based
                        factory.getEngineName(),
                        factory.getEngineVersion(),
                        factory.getLanguageName(),
                        factory.getLanguageVersion(),
                        ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class ScriptEngineNamesISFactory extends BasicFactoryBase {
        public ScriptEngineNamesISFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public long rowCount(Session session) {
            int c = 0;
            for (ScriptEngineFactory factory : getScriptEngineFactories())
                c += factory.getNames().size();
            return c;
        }

        @Override
        public GroupScan getGroupScan(VirtualAdapter adapter, Group group) {
            return new Scan(getScriptEngineFactories().listIterator(), getRowType( group.getAIS()));
        }

        private class Scan extends BaseScan {
            final ListIterator<ScriptEngineFactory> factories;
            Iterator<String> names;

            public Scan(ListIterator<ScriptEngineFactory> factories, RowType rowType) {
                super(rowType);
                this.factories = factories;
                this.names = Iterators.emptyIterator();
            }

            @Override
            public Row next() {
                while (!names.hasNext()) {
                    if (!factories.hasNext())
                        return null;
                    names = factories.next().getNames().iterator();
                }
                String nextName = names.next();
                return new ValuesHolderRow(
                        rowType,
                        nextName,
                        factories.nextIndex(),      // use nextIndex so that the IDs are 1-based
                        ++rowCounter /*hidden pk*/); 
            }
        }
    }

    private class TypeAttributesFactory extends BasicFactoryBase {
        public TypeAttributesFactory (TableName sourceTable) {
            super (sourceTable);
        }
        @Override
        public long rowCount(Session session) {
            return getTypesRegistry().getTypeClasses().size();
        }
        @Override 
        public GroupScan getGroupScan (VirtualAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }
        
        private class Scan extends BaseScan {
            final Iterator<? extends TClass> typesList;
            Iterator<? extends Attribute> attrs = null;
            TClass currType = null;
           
            public Scan (RowType rowType) {
                super (rowType);
                typesList = getTypesRegistry().getTypeClasses().iterator();
            }
            
            private TClass nextType() {
                TClass type = null;
                do {
                    if (!typesList.hasNext()) 
                        return null;
                    type = typesList.next();
                } while (!TypeValidator.isSupportedForColumn(type));
                return type;
            }
           
            @Override
            public Row next() {
                if (attrs == null || !attrs.hasNext()) {
                    do {
                        if ((currType = nextType()) == null) 
                            return null;
                        attrs = currType.attributes().iterator();
                    } while (!attrs.hasNext());
                }
                Attribute attr = attrs.next();
                return new ValuesHolderRow (rowType,
                        currType.name().unqualifiedName(),
                        attr.name(),
                        ++rowCounter);
            }
        }
    }
    
    private class TypeBundlesFactory extends BasicFactoryBase {
        public TypeBundlesFactory (TableName sourceTable) {
            super (sourceTable);
        }
        
        @Override
        public long rowCount(Session session) {
            return getTypesRegistry().getTypeBundleIDs().size();
        }
        
        @Override
        public GroupScan getGroupScan (VirtualAdapter adapter, Group group) {
            return new Scan(getRowType( group.getAIS()));
        }
        
        private class Scan extends BaseScan {
            final Iterator<? extends TBundleID> bundlesList;
            public Scan (RowType rowType) {
                super(rowType);
                bundlesList = getTypesRegistry().getTypeBundleIDs().iterator();
            }
            
            @Override
            public Row next() {
                if (!bundlesList.hasNext()) 
                    return null;

                TBundleID bundle = bundlesList.next();
                return new ValuesHolderRow (rowType,
                                      bundle.name(),
                                      bundle.uuid().toString(),
                                      ++rowCounter);
            }
        }
    }

    private class TypeFactory extends BasicFactoryBase {
        public TypeFactory (TableName sourceTable) {
            super(sourceTable);
        }
        
        @Override
        public long rowCount(Session session) {
            return getTypesRegistry().getTypeClasses().size();
        }
        
        @Override
        public GroupScan getGroupScan (VirtualAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }
        
        private class Scan extends BaseScan {
            final Iterator<? extends TClass> typesList;
            public Scan (RowType rowType) {
                super(rowType);
                typesList = getTypesRegistry().getTypeClasses().iterator();
            }
            
            @Override
            public Row next() {
                // Skip the unsupported types
                TClass tClass = null;
                do {
                    if (!typesList.hasNext()) 
                        return null;
                    tClass = typesList.next();
                } while (!TypeValidator.isSupportedForColumn(tClass));
                
                boolean indexable = TypeValidator.isSupportedForIndex(tClass);
                Long pgType = null;
                if (postgresTypeMapper != null) {
                    TInstance type = tClass.instance(true);
                    pgType = postgresTypeMapper.getOid(type);
                }
                
                String bundle = tClass.name().bundleId().name();
                String category = tClass.name().categoryName();
                String name = tClass.name().unqualifiedName();
                
                String attribute1 = null;
                String attribute2 = null;
                String attribute3 = null;
                Iterator<? extends Attribute> attrs = tClass.attributes().iterator();
                if (attrs.hasNext()) {
                    attribute1 = attrs.next().name();
                }
                if (attrs.hasNext()) {
                    attribute2 = attrs.next().name();
                }
                if (attrs.hasNext()) {
                    attribute3 = attrs.next().name();
                }
                Long size = tClass.hasFixedSerializationSize() ? (long)tClass.fixedSerializationSize() : null;
                
                Integer jdbcTypeID = tClass.jdbcType();
                
                return new ValuesHolderRow (rowType,
                        name,
                        category,
                        bundle,
                        attribute1, 
                        attribute2,
                        attribute3,
                        size,
                        pgType,
                        (long)jdbcTypeID,
                        boolResult(indexable),
                        ++rowCounter);
            }
        }
    }
    
    //
    // Package, for testing
    //

    static AkibanInformationSchema createTablesToRegister(TypesTranslator typesTranslator) {
        // bug1127376: Grouping constraint names are auto-generated and very long. Use a big value until that changes.
        final int GROUPING_CONSTRAINT_MAX = PATH_MAX;

        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator);
        builder.table(SCHEMATA)
                .colString("catalog_name", IDENT_MAX, true)
                .colString("schema_name", IDENT_MAX, false)
                .colString("schema_owner", IDENT_MAX, true)
                .colString("default_character_set_catalog", IDENT_MAX, true)
                .colString("default_character_set_schema", IDENT_MAX, true)
                .colString("default_character_set_name", IDENT_MAX, true)
                .colString("sql_path", PATH_MAX, true)
                .colString("default_collation_catalog", IDENT_MAX, true)
                .colString("default_collation_schema", IDENT_MAX, true)
                .colString("default_collation_name", IDENT_MAX, true);
        //primary key (schema_name)
        builder.table(TABLES)
                .colString("table_catalog", IDENT_MAX, true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("table_type", IDENT_MAX, false)
                .colString("self_referencing_column", IDENT_MAX, true)
                .colText("reference_generation", true)
                .colString("is_insertable_into", YES_NO_MAX, false)
                .colString("is_typed", YES_NO_MAX, false)
                .colString("commit_action", DESCRIPTOR_MAX, true)
                .colString("default_character_set_catalog", IDENT_MAX, true)
                .colString("default_character_set_schema", IDENT_MAX, true)
                .colString("default_character_set_name", IDENT_MAX, true)
                .colString("default_collation_catalog", IDENT_MAX, true)
                .colString("default_collation_schema", IDENT_MAX, true)
                .colString("default_collation_name", IDENT_MAX, true)
                .colBigInt("table_id", true)
                .colBigInt("group_ordinal", true)
                .colString("storage_name", PATH_MAX, true)
                .colString("storage_format", IDENT_MAX, true);
        //primary key (table_schema, table_name)
        //foreign_key (table_schema) references SCHEMATA (schema_name)
        //foreign key (character_set_schema, character_set_name) references CHARACTER_SETS
        //foreign key (collations_schema, collation_name) references COLLATIONS
        //foreign key (storage_name) references STORAGE_TREES
        builder.table(COLUMNS)
                .colString("table_catalog", IDENT_MAX, true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false)
                .colBigInt("ordinal_position", false)
                .colString("column_default", PATH_MAX, true)
                .colString("is_nullable", YES_NO_MAX, false)
                .colString("data_type", DESCRIPTOR_MAX, false)
                .colBigInt("character_maximum_length", true)
                .colBigInt("character_octet_length", true)
                .colBigInt("numeric_precision", true)
                .colBigInt("numeric_precision_radix", true)
                .colBigInt("numeric_scale", true)
                .colString("character_set_catalog", IDENT_MAX, true)
                .colString("character_set_schema", IDENT_MAX, true)
                .colString("character_set_name", IDENT_MAX, true)
                .colString("collation_catalog", IDENT_MAX, true)
                .colString("collation_schema", IDENT_MAX, true)
                .colString("collation_name", IDENT_MAX, true)
                .colString("domain_catalog", IDENT_MAX, true)
                .colString("domain_schema", IDENT_MAX, true)
                .colString("domain_name", IDENT_MAX, true)
                .colString("udt_catalog",IDENT_MAX, true)
                .colString("udt_schema", IDENT_MAX, true)
                .colString("udt_name", IDENT_MAX, true)
                .colString("scope_catalog", IDENT_MAX, true)
                .colString("scope_schema", IDENT_MAX, true)
                .colString("scope_name", IDENT_MAX, true)
                .colBigInt("maximum_cardinality", true)
                .colString("is_self_referencing", YES_NO_MAX, false)
                .colString("is_identity", YES_NO_MAX, false)
                .colString("identity_generation", IDENT_MAX, true)
                .colBigInt("identity_start", true)
                .colBigInt("identity_increment", true)
                .colBigInt("identity_maximum", true)
                .colBigInt("identity_minimum", true)
                .colString("identity_cycle", YES_NO_MAX, true)
                .colString("is_generated", YES_NO_MAX, false)
                .colString("generation_expression", PATH_MAX, true)
                .colString("is_updatable", YES_NO_MAX, true)
                .colString("sequence_catalog", IDENT_MAX, true)
                .colString("sequence_schema", IDENT_MAX, true)
                .colString("sequence_name", IDENT_MAX, true);
        //primary key(table_schema, table_name, column_name)
        //foreign key(table_schema, table_name) references TABLES (table_schema, table_name)
        //foreign key (type) references TYPES (type_name)
        //foreign key (character_set_schema, character_set_name) references CHARACTER_SETS
        //foreign key (collation_schema, collation_name) references COLLATIONS
        builder.table(TABLE_CONSTRAINTS)
                .colString("constraint_catalog", IDENT_MAX, true)
                .colString("constraint_schema", IDENT_MAX, false)
                .colString("constraint_name", GROUPING_CONSTRAINT_MAX, false)
                .colString("table_catalog", IDENT_MAX, true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("constraint_type", DESCRIPTOR_MAX, false)
                .colString("is_deferrable", YES_NO_MAX, false)
                .colString("initially_deferred", YES_NO_MAX, false)
                .colString("enforced", YES_NO_MAX, false);
        //primary key (constraint_schema, constraint_table, constraint_name)
        //foreign key (table_schema, table_name) references TABLES
        builder.table(REFERENTIAL_CONSTRAINTS)
            .colString("constraint_catalog", IDENT_MAX, true)
            .colString("constraint_schema", IDENT_MAX, false)
            .colString("constraint_name", IDENT_MAX, false)
            .colString("unique_constraint_catalog", IDENT_MAX, true)
            .colString("unique_constraint_schema", IDENT_MAX, false)
            .colString("unique_constraint_name", IDENT_MAX, false)
            .colString("match_option", DESCRIPTOR_MAX, false)
            .colString("update_rule", DESCRIPTOR_MAX, false)
            .colString("delete_rule", DESCRIPTOR_MAX, false);
        //foreign key (constraint_schema, constraint_name)
        //    references TABLE_CONSTRAINTS (constraint_schema, constraint_name)
        builder.table(GROUPING_CONSTRAINTS) 
                .colString("root_table_catalog", IDENT_MAX, true)
                .colString("root_table_schema", IDENT_MAX, false)
                .colString("root_table_name", IDENT_MAX, false)
                .colString("constraint_catalog",IDENT_MAX, true)
                .colString("constraint_schema", IDENT_MAX, false)
                .colString("constraint_table_name", IDENT_MAX, false)
                .colString("path", IDENT_MAX, false)
                .colBigInt("depth", false)
                .colString("constraint_name", GROUPING_CONSTRAINT_MAX, true)
                .colString("unique_catalog", IDENT_MAX, true)
                .colString("unique_schema", IDENT_MAX, true)
                .colString("unique_constraint_name", GROUPING_CONSTRAINT_MAX, true);
        //foreign key (constraint_schema, constraint_name)
        //    references TABLE_CONSTRAINTS (constraint_schema, constraint_name)
        builder.table(KEY_COLUMN_USAGE)
            .colString("constraint_catalog", IDENT_MAX, true)
            .colString("constraint_schema", IDENT_MAX, false)
            .colString("constraint_name", GROUPING_CONSTRAINT_MAX, false)
            .colString("table_catalog", IDENT_MAX, true)
            .colString("table_schema", IDENT_MAX, false)
            .colString("table_name", IDENT_MAX, false)
            .colString("column_name", IDENT_MAX, false)
            .colBigInt("ordinal_position", false)
            .colBigInt("position_in_unique_constraint", true);
        //primary key  (constraint_schema, constraint_name, column_name),
        //foreign key (constraint_schema, constraint_name) references TABLE_CONSTRAINTS
        builder.table(INDEXES)
                .colString("table_catalog", IDENT_MAX, true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("index_name", IDENT_MAX, false)
                .colString("constraint_catalog", IDENT_MAX, true)
                .colString("constraint_schema", IDENT_MAX, true)
                .colString("constraint_name", IDENT_MAX, true)
                .colBigInt("index_id", false)
                .colString("storage_name", PATH_MAX, true)
                .colString("storage_format", IDENT_MAX, true)
                .colString("index_type", DESCRIPTOR_MAX, false)
                .colString("is_unique", YES_NO_MAX, false)
                .colString("join_type", DESCRIPTOR_MAX, true)
                .colString("index_method", IDENT_MAX, true);
        //primary key(table_schema, table_name, index_name)
        //foreign key (constraint_schema, constraint_name)
        //    references TABLE_CONSTRAINTS (constraint_schema, constraint_name)
        //foreign key (table_schema, table_name) references TABLES (table_schema, table_name)
        builder.table(INDEX_COLUMNS)
                .colString("index_table_catalog", IDENT_MAX, true)
                .colString("index_table_schema", IDENT_MAX, false)
                .colString("index_table_name", IDENT_MAX, false)
                .colString("index_name", IDENT_MAX, false)
                .colString("column_catalog", IDENT_MAX, true)
                .colString("column_schema", IDENT_MAX, false)
                .colString("column_table", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false)
                .colBigInt("ordinal_position", false)
                .colString("is_ascending", IDENT_MAX, false);
        //primary key(index_table_schema, index_table, index_name, column_schema, column_table, column_name)
        //foreign key(index_table_schema, index_table_name, index_name)
        //    references INDEXES (table_schema, table_name, index_name)
        //foreign key (column_schema, column_table, column_name)
        //    references COLUMNS (table_schema, table_name, column_name)
        builder.table(SEQUENCES)
                .colString("sequence_catalog", IDENT_MAX, true)
                .colString("sequence_schema", IDENT_MAX, false)
                .colString("sequence_name", IDENT_MAX, false)
                .colString("data_type", DESCRIPTOR_MAX, false)
                .colBigInt("start_value", false)
                .colBigInt("minimum_value", false)
                .colBigInt("maximum_value", false)
                .colBigInt("increment", false)
                .colString("cycle_option", YES_NO_MAX, false)
                .colString("storage_name", IDENT_MAX, false);
        //primary key (sequence_schema, sequence_name)
        //foreign key (data_type) references type (type_name)
                
        builder.table(VIEWS)
                .colString("table_catalog", IDENT_MAX,true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colText("view_definition", false)
                .colString("check_option", DESCRIPTOR_MAX, false)
                .colString("is_updatable", YES_NO_MAX, false)
                .colString("is_insertable_into", YES_NO_MAX, false)
                .colString("is_trigger_updatable", YES_NO_MAX, false)
                .colString("is_trigger_deletable", YES_NO_MAX, false)
                .colString("is_trigger_insertable_into", YES_NO_MAX, false);
        //primary key(table_schema, table_name)
        //foreign key(table_schema, table_name) references TABLES (table_schema, table_name)

        builder.table(VIEW_TABLE_USAGE)
                .colString("view_catalog", IDENT_MAX, true)
                .colString("view_schema", IDENT_MAX, false)
                .colString("view_name", IDENT_MAX, false)
                .colString("table_catalog", IDENT_MAX, true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false);
        //foreign key(view_schema, view_name) references VIEWS (schema_name, table_name)
        //foreign key(table_schema, table_name) references TABLES (schema_name, table_name)

        builder.table(VIEW_COLUMN_USAGE)
                .colString("view_catalog", IDENT_MAX, true)
                .colString("view_schema", IDENT_MAX, false)
                .colString("view_name", IDENT_MAX, false)
                .colString("table_catalog", IDENT_MAX, true)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false);
        //foreign key(view_schema, view_name) references VIEWS (schema_name, table_name)
        //foreign key(table_schema, table_name, column_name) references COLUMNS
 
        builder.table(ROUTINES)
                .colString("specific_catalog", IDENT_MAX,true)
                .colString("specific_schema", IDENT_MAX, false)
                .colString("specific_name", IDENT_MAX, false)
                .colString("routine_catalog", IDENT_MAX, true)
                .colString("routine_schema", IDENT_MAX, false)
                .colString("routine_name", IDENT_MAX, false)
                .colString("routine_type", DESCRIPTOR_MAX, false)
                .colString("module_catalog", IDENT_MAX, true)
                .colString("module_schema", IDENT_MAX, true)
                .colString("module_name", IDENT_MAX, true)
                .colString("udt_catalog", IDENT_MAX, true)
                .colString("udt_schema", IDENT_MAX, true)
                .colString("udt_name", IDENT_MAX, true)
                .colString("routine_body", DESCRIPTOR_MAX, true)
                .colText("routine_definition", true)
                .colString("external_name", PATH_MAX, true)
                .colString("language", IDENT_MAX, false)
                .colString("parameter_style", IDENT_MAX, false)
                .colString("is_deterministic", YES_NO_MAX, false)
                .colString("sql_data_access", IDENT_MAX, true)
                .colString("is_null_call", YES_NO_MAX, true)
                .colString("sql_path", PATH_MAX, true)
                .colString("schema_level_routine", YES_NO_MAX, false)
                .colBigInt("max_dynamic_result_sets", false)
                .colString("is_user_defined_cast", YES_NO_MAX, true)
                .colString("is_implicitly_invokable", YES_NO_MAX, true)
                .colString("security_type", DESCRIPTOR_MAX, true)
                .colString("as_locator", YES_NO_MAX, true)
                .colString("is_udt_dependent", YES_NO_MAX, true)
                .colSystemTimestamp("created", true)
                .colSystemTimestamp("last_updated", true)
                .colString("new_savepoint_level", YES_NO_MAX,true);
        //primary key(routine_schema, routine_name)

        builder.table(PARAMETERS)
                .colString("specific_catalog", IDENT_MAX,true)
                .colString("specific_schema", IDENT_MAX, false)
                .colString("specific_name", IDENT_MAX, false)
                .colString("parameter_name", IDENT_MAX, true)
                .colBigInt("ordinal_position", false)
                .colString("data_type", DESCRIPTOR_MAX, false)
                .colBigInt("character_maximum_length", true)
                .colBigInt("numeric_precision", true)
                .colBigInt("numeric_precision_radix", true)
                .colBigInt("numeric_scale",true)
                .colString("parameter_mode", DESCRIPTOR_MAX, false)
                .colString("is_result", YES_NO_MAX, false)
                .colString("parameter_default", PATH_MAX, true);
        //primary key(specific_schema, specific_name, parameter_name, ordinal_position)
        //foreign key(routine_schema, routine_name) references ROUTINES (routine_schema, routine_name)
        //foreign key (type) references TYPES (type_name)

        builder.table(JARS)
                .colString("jar_catalog", IDENT_MAX, true)
                .colString("jar_schema", IDENT_MAX, false)
                .colString("jar_name", IDENT_MAX, false)
                .colString("java_path", PATH_MAX, true);
        //primary key(jar_schema, jar_name)

        builder.table(ROUTINE_JAR_USAGE)
                .colString("specific_catalog", IDENT_MAX, true)
                .colString("specific_schema", IDENT_MAX, false)
                .colString("specific_name", IDENT_MAX, false)
                .colString("jar_catalog", IDENT_MAX, true)
                .colString("jar_schema", IDENT_MAX, false)
                .colString("jar_name", IDENT_MAX, false);
        //foreign key(specific_schema, specific_name) references ROUTINES (specific_schema, specific_name)
        //foreign key(jar_schema, jar_name) references JARS (jar_schema, jar_name)

        builder.table(SCRIPT_ENGINES)
                .colInt("engine_id", false)
                .colString("engine_name", IDENT_MAX, false)
                .colString("engine_version", IDENT_MAX, false)
                .colString("language_name", IDENT_MAX, false)
                .colString("language_version", IDENT_MAX, false);
        //primary key(engine_id)

        builder.table(SCRIPT_ENGINE_NAMES)
                .colString("name", IDENT_MAX, false)
                .colInt("engine_id", false);
        //foreign key (engine_id) references SCRIPT_ENGINES (engine_id)

        builder.table(TYPES)
            .colString("type_name", IDENT_MAX, false)
            .colString("type_category", IDENT_MAX, false)
            .colString("type_bundle_name", IDENT_MAX)
            .colString("attribute_1", IDENT_MAX)
            .colString("attribute_2", IDENT_MAX)
            .colString("attribute_3", IDENT_MAX)
            .colInt("fixed_length")
            .colInt("postgres_oid")
            .colInt("jdbc_type_id")
            .colString("indexable", YES_NO_MAX);

        builder.table(TYPE_BUNDLES)
            .colString("type_bundle_name", IDENT_MAX, false)
            .colString("bundle_guid", IDENT_MAX, false);
        
        builder.table(TYPE_ATTRIBUTES)
            .colString("type_name", IDENT_MAX, false)
            .colString("attribute_name", IDENT_MAX, false);
        
        return builder.ais(false);
    }

    void attachFactories(AkibanInformationSchema ais) {
        attach(ais, SCHEMATA, SchemataFactory.class);
        attach(ais, TABLES, TablesFactory.class);
        attach(ais, COLUMNS, ColumnsFactory.class);
        attach(ais, TABLE_CONSTRAINTS, TableConstraintsFactory.class);
        attach(ais, REFERENTIAL_CONSTRAINTS, ReferentialConstraintsFactory.class);
        attach(ais, GROUPING_CONSTRAINTS, GroupingConstraintsFactory.class);
        attach(ais, KEY_COLUMN_USAGE, KeyColumnUsageFactory.class);
        attach(ais, INDEXES, IndexesFactory.class);
        attach(ais, INDEX_COLUMNS, IndexColumnsFactory.class);
        attach(ais, SEQUENCES, SequencesFactory.class);
        attach(ais, VIEWS, ViewsFactory.class);
        attach(ais, VIEW_TABLE_USAGE, ViewTableUsageFactory.class);
        attach(ais, VIEW_COLUMN_USAGE, ViewColumnUsageFactory.class);
        attach(ais, ROUTINES, RoutinesFactory.class);
        attach(ais, PARAMETERS, ParametersFactory.class);
        attach(ais, JARS, JarsFactory.class);
        attach(ais, ROUTINE_JAR_USAGE, RoutineJarUsageFactory.class);
        attach(ais, SCRIPT_ENGINES, ScriptEnginesISFactory.class);
        attach(ais, SCRIPT_ENGINE_NAMES, ScriptEngineNamesISFactory.class);
        attach(ais, TYPES, TypeFactory.class);
        attach(ais, TYPE_BUNDLES, TypeBundlesFactory.class);
        attach(ais, TYPE_ATTRIBUTES, TypeAttributesFactory.class);
    }
}
