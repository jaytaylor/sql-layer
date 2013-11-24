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
import com.foundationdb.ais.model.CharsetAndCollation;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.FullTextIndex;
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
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.foundationdb.qp.row.ValuesRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.rowdata.RowDefCache;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.routines.ScriptEngineManagerProvider;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.mcompat.MBundle;
import com.foundationdb.server.types.mcompat.mtypes.MBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.sql.pg.PostgresType;
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

    private static final String CHARSET_SCHEMA = SCHEMA_NAME;
    private static final String COLLATION_SCHEMA = SCHEMA_NAME;

    private final SecurityService securityService;
    private final ScriptEngineManagerProvider scriptEngineManagerProvider;

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
        AkibanInformationSchema ais = createTablesToRegister();
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

    protected boolean isAccessible(Session session, String schemaName) {
        if ((session == null) || (securityService == null))
            return true;
        return securityService.isAccessible(session, schemaName);
    }

    protected boolean isAccessible(Session session, TableName name) {
        return isAccessible(session, name.getSchemaName());
    }

    private List<ScriptEngineFactory> getScriptEngineFactories() {
        return scriptEngineManagerProvider.getManager().getEngineFactories();
    }

    private class SchemataFactory extends BasicFactoryBase {
        public SchemataFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return getDirtyAIS().getSchemas().size();
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
                        return new ValuesRow(rowType,
                                             schema.getName(),
                                             null, // owner
                                             null, // default charset schema
                                             null, // default charset name
                                             null, // collation
                                             ++rowCounter /*hidden pk*/); 
                    }
                }
                return null;
           }
        }
    }

    private AkibanInformationSchema getDirtyAIS() {
        return RowDefCache.latestForDebugging().ais();
    }

    private AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    private class TablesFactory extends BasicFactoryBase {
        public TablesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            AkibanInformationSchema ais = getDirtyAIS();
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
                        final String tableType = table.hasMemoryTableFactory() ? "DICTIONARY VIEW" : "TABLE";
                        final Integer ordinal = table.hasMemoryTableFactory() ? null : table.getOrdinal();
                        if(isAccessible(session, table.getName())) {
                            return new ValuesRow(rowType,
                                                 table.getName().getSchemaName(),
                                                 table.getName().getTableName(),
                                                 tableType,
                                                 table.getTableId(),
                                                 ordinal,
                                                 table.getGroup().getStorageNameString(),
                                                 CHARSET_SCHEMA,
                                                 table.getCharsetAndCollation().charset(),
                                                 COLLATION_SCHEMA,
                                                 table.getCharsetAndCollation().collation(),
                                                 ++rowCounter /*hidden pk*/);
                        }
                    }
                    viewIt = getAIS(session).getViews().values().iterator();
                }
                while(viewIt.hasNext()) {
                    View view = viewIt.next();
                    if(isAccessible(session, view.getName())) {
                        return new ValuesRow(rowType,
                                             view.getName().getSchemaName(),
                                             view.getName().getTableName(),
                                             "VIEW",
                                             null,
                                             null,
                                             null,
                                             null,
                                             null,
                                             null,
                                             null,
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            AkibanInformationSchema ais = getDirtyAIS();
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
                final Long length;
                if(column.getType().fixedSize()) {
                    length = column.getMaxStorageSize();
                } else {
                    length = column.getTypeParameter1();
                }

                // TODO: This should come from type attributes when new types go in

                Long precision = null;
                Long scale = null;
                Long radix = null;
                CharsetAndCollation charAndColl = null;
                if (column.tInstance().typeClass() instanceof MBigDecimal) {
                    precision = (long) column.tInstance().attribute(MBigDecimal.Attrs.PRECISION);
                    scale = (long) column.tInstance().attribute(MBigDecimal.Attrs.SCALE);
                    radix = new Long (10);
                }
                Long charMaxLength = null;
                Long charOctetLength = null;
                if (column.tInstance().typeClass() instanceof TString) {
                    charAndColl = column.getCharsetAndCollation();
                    charMaxLength = length;
                    charOctetLength = column.getMaxStorageSize(); 
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
                
                return new ValuesRow(rowType,
                                     column.getColumnar().getName().getSchemaName(),
                                     column.getColumnar().getName().getTableName(),
                                     column.getName(),
                                     column.getPosition().longValue(),
                                     defaultString,
                                     boolResult(column.getNullable()),
                                     column.getType().name(),
                                     charMaxLength,
                                     charOctetLength,
                                     precision,
                                     radix,
                                     scale,
                                     charAndColl != null ? CHARSET_SCHEMA : null,
                                     charAndColl != null ? charAndColl.charset() : null,
                                     charAndColl != null ? COLLATION_SCHEMA : null,
                                     charAndColl != null ? charAndColl.collation() : null,
                                     identityGeneration,
                                     identityGeneration != null ? identityStart : null,
                                     identityGeneration != null ? identityIncrement : null,
                                     identityGeneration != null ? identityMin : null,
                                     identityGeneration != null ? identityMax : null,
                                     identityCycle,
                                     boolResult(true),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            AkibanInformationSchema ais = getDirtyAIS();
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
                return new ValuesRow(rowType,
                                     it.getTable().getName().getSchemaName(),
                                     it.getTable().getName().getTableName(),
                                     it.getName(),
                                     it.getType(),
                                     boolResult(false),
                                     boolResult(false),
                                     boolResult(true),
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class ReferentialConstraintsFactory extends BasicFactoryBase {
        public ReferentialConstraintsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new ConstraintsScan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 0;
        }
        private class ConstraintsScan extends BaseScan {

            public ConstraintsScan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                return null;
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return getDirtyAIS().getTables().size();
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
                String uniqueTable = null;
                String uniqueConstraint = null;

                Join join = table.getParentJoin();
                if (table.getParentJoin() != null) {
                    constraintName = join.getName();
                    uniqueSchema = join.getParent().getName().getSchemaName();
                    uniqueTable = join.getParent().getName().getTableName();
                    uniqueConstraint = Index.PRIMARY_KEY_CONSTRAINT;
                }

                return new ValuesRow(rowType,
                                     rpt.root.getName().getSchemaName(),// root_schema_name
                                     rpt.root.getName().getTableName(), // root_table_name
                                     table.getName().getSchemaName(),   // constraint_schema_name
                                     table.getName().getTableName(),    // constraint_table_name
                                     rpt.path,                          // path
                                     table.getDepth().longValue(),      // depth
                                     constraintName,                    // constraint_name
                                     uniqueSchema,                      // unique_schema_name
                                     uniqueTable,                       // unique_table_name
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            int count = 0;
            TableConstraintsIteration it = newIteration(null, getDirtyAIS());
            while(it.next()) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final TableConstraintsIteration it;
            Iterator<IndexColumn> indexColIt;
            Iterator<JoinColumn> joinColIt;
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
                posInUnique = null;
                if(joinColIt != null && joinColIt.hasNext()) {
                    JoinColumn joinColumn = joinColIt.next();
                    colName = joinColumn.getChild().getName();
                    posInUnique = findPosInIndex(joinColumn.getParent(), joinColumn.getParent().getTable().getPrimaryKey().getIndex()).longValue();
                } else if(indexColIt != null && indexColIt.hasNext()) {
                    IndexColumn indexColumn = indexColIt.next();
                    colName = indexColumn.getColumn().getName();
                } else if(it.next()) {
                    joinColIt = null;
                    indexColIt = null;
                    if(it.isGrouping()) {
                        joinColIt = it.getTable().getParentJoin().getJoinColumns().iterator();
                    } else {
                        indexColIt = it.getIndex().getKeyColumns().iterator();
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
                return new ValuesRow(rowType,
                                     it.getTable().getName().getSchemaName(),
                                     it.getTable().getName().getTableName(),
                                     it.getName(),
                                     colName,
                                     new Long(colPos),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            IndexIteration it = newIteration(null, getDirtyAIS());
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
                String constraintName = null;
                if(index.isPrimaryKey()) {
                    indexType = constraintName = Index.PRIMARY_KEY_CONSTRAINT;
                } else if(index.isUnique()) {
                    constraintName = index.getIndexName().getName();
                    indexType = Index.UNIQUE_KEY_CONSTRAINT;
                } else {
                    indexType = "INDEX";
                }
                return new ValuesRow(rowType,
                                     indexIt.getTable().getName().getSchemaName(),
                                     indexIt.getTable().getName().getTableName(),
                                     index.getIndexName().getName(),
                                     constraintName,
                                     index.getIndexId(),
                                     index.getStorageNameString(),
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
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            IndexIteration indexIt = newIteration(null, getDirtyAIS());
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
                return new ValuesRow(rowType,
                                     indexIt.getTable().getName().getSchemaName(),
                                     indexColumn.getIndex().getIndexName().getName(),
                                     indexIt.getTable().getName().getTableName(),
                                     indexColumn.getColumn().getTable().getName().getSchemaName(),
                                     indexColumn.getColumn().getTable().getName().getTableName(),
                                     indexColumn.getColumn().getName(),
                                     indexColumn.getPosition().longValue(),
                                     boolResult(indexColumn.isAscending()),
                                     indexColumn.getIndexedLength(),
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class SequencesFactory extends BasicFactoryBase {
        public SequencesFactory (TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return getDirtyAIS().getSequences().size();
        }
        
        private class Scan extends BaseScan {
            final Session session;
            final Iterator<Sequence> it;
            
            public Scan(Session session, RowType rowType) {
                super(rowType);
                this.session = session;
                this.it =  getAIS(session).getSequences().values().iterator();
            }

            @Override
            public Row next() {
                while(it.hasNext()) {
                    Sequence sequence = it.next();
                    if(isAccessible(session, sequence.getSequenceName())) {
                        return new ValuesRow(rowType,
                                             sequence.getSequenceName().getSchemaName(),
                                             sequence.getSequenceName().getTableName(),
                                             sequence.getStorageNameString(),
                                             sequence.getStartsWith(),
                                             sequence.getIncrement(),
                                             sequence.getMinValue(),
                                             sequence.getMaxValue(),
                                             boolResult(sequence.isCycle()),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return getDirtyAIS().getViews().size();
        }

        private class Scan extends BaseScan {
            final Session session;
            final Iterator<View> it;

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
                        return new ValuesRow(rowType,
                                             view.getName().getSchemaName(),
                                             view.getName().getTableName(),
                                             view.getDefinition(),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (View view : getDirtyAIS().getViews().values()) {
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
                return new ValuesRow(rowType,
                                     view.getName().getSchemaName(),
                                     view.getName().getTableName(),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (View view : getDirtyAIS().getViews().values()) {
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
                return new ValuesRow(rowType,
                                     view.getName().getSchemaName(),
                                     view.getName().getTableName(),
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
        private Iterator<? extends Index> indexIt;
        private Table curTable;
        private Index curIndex;
        private String name;
        private String type;

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
                    if(curTable.getParentJoin() != null) {
                        name = curTable.getParentJoin().getName(); // TODO: Need a real constraint name here
                        type = "GROUPING";
                        return true;
                    }
                }
                if(indexIt == null) {
                    indexIt = curTable.getIndexes().iterator();
                }
                while(indexIt.hasNext()) {
                    curIndex = indexIt.next();
                    if(curIndex.isUnique()) {
                        name = curIndex.getIndexName().getName();
                        type = curIndex.isPrimaryKey() ? "PRIMARY KEY" : curIndex.getConstraint();
                        return true;
                    }
                }
                indexIt = null;
                curIndex = null;
                curTable = null;
            }
            return false;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public Table getTable() {
            return curTable;
        }

        public Index getIndex() {
            return curIndex;
        }

        public boolean isGrouping() {
            return indexIt == null;
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return getDirtyAIS().getRoutines().size() ;
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
                        return new ValuesRow(rowType,
                                             routine.getName().getSchemaName(),
                                             routine.getName().getTableName(),
                                             routine.isProcedure() ? "PROCEDURE" : "FUNCTION",
                                             routine.getDefinition(),
                                             routine.getExternalName(),
                                             routine.getLanguage(),
                                             routine.getCallingConvention().name(),
                                             boolResult(false),
                                             (routine.getSQLAllowed() == null) ? null : routine.getSQLAllowed().name().replace('_', ' '),
                                             boolResult(true),
                                             (long)(routine.getDynamicResultSets()),
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (Routine routine : getDirtyAIS().getRoutines().values()) {
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

                if (param.tInstance().typeClass() == MString.CHAR ||
                    param.tInstance().typeClass() == MString.VARCHAR)
                {
                    length = param.getTypeParameter1();
                } else if (param.tInstance().typeClass() == MNumeric.DECIMAL ||
                            param.tInstance().typeClass() == MNumeric.DECIMAL_UNSIGNED) {
                    precision = param.getTypeParameter1();
                    scale = param.getTypeParameter2();
                }
                return new ValuesRow(rowType,
                                     param.getRoutine().getName().getSchemaName(),
                                     param.getRoutine().getName().getTableName(),
                                     param.getName(),
                                     ordinal,
                                     param.getType().name(),
                                     length, 
                                     precision, 
                                     scale,
                                     (param.getDirection() == Parameter.Direction.RETURN) ? "OUT" : param.getDirection().name(),
                                     boolResult(param.getDirection() == Parameter.Direction.RETURN),
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class JarsFactory extends BasicFactoryBase {
        public JarsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return getDirtyAIS().getSQLJJars().size() ;
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
                        return new ValuesRow(rowType,
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
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(adapter.getSession(), getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (Routine routine : getDirtyAIS().getRoutines().values()) {
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
                        return new ValuesRow(rowType,
                                             routine.getName().getSchemaName(),
                                             routine.getName().getTableName(),
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
        public long rowCount() {
            return getScriptEngineFactories().size();
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getScriptEngineFactories().listIterator(), getRowType(adapter));
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
                return new ValuesRow(
                        rowType,
                        it.nextIndex(), // use nextIndex so that the IDs are 1-based
                        factory.getEngineName(),
                        factory.getEngineVersion(),
                        factory.getLanguageName(),
                        factory.getLanguageVersion());
            }
        }
    }

    private class ScriptEngineNamesISFactory extends BasicFactoryBase {
        public ScriptEngineNamesISFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public long rowCount() {
            int c = 0;
            for (ScriptEngineFactory factory : getScriptEngineFactories())
                c += factory.getNames().size();
            return c;
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getScriptEngineFactories().listIterator(), getRowType(adapter));
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
                return new ValuesRow(
                        rowType,
                        nextName,
                        factories.nextIndex()); // use nextIndex so that the IDs are 1-based
            }
        }
    }

    private class TypeAttributesFactory extends BasicFactoryBase {
        public TypeAttributesFactory (TableName sourceTable) {
            super (sourceTable);
        }
        @Override
        public long rowCount() {
            return Types.types().size() - Types.unsupportedTypes().size();
        }
        @Override 
        public GroupScan getGroupScan (MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }
        
        private class Scan extends BaseScan {
            final Iterator<Type> typesList;
            Iterator<? extends Attribute> attrs = null;
            Type currType = null;
           
            public Scan (RowType rowType) {
                super (rowType);
                typesList = Types.types().iterator();
            }
            
            private Type nextType() {
                Type type = null;
                do {
                    if (!typesList.hasNext()) 
                        return null;
                    type = typesList.next();
                } while (Types.unsupportedTypes().contains(type));
                return type;
            }
           
            @Override
            public Row next() {
                
                if (!typesList.hasNext())
                    return null;

                Attribute attr = null;
                
                if (attrs == null || !attrs.hasNext()) {
                    do {
                        if ((currType = nextType()) == null) 
                            return null;
                        TInstance tInstance = Column.generateTInstance(null, currType, 1L, 1L, true);
                        attrs = tInstance.typeClass().attributes().iterator();
                    } while (!attrs.hasNext());
                }
                attr = attrs.next();
                
                return new ValuesRow (rowType,
                        currType.name(),
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
        public long rowCount() {
            return 2; // The two default ones. 
        }
        
        @Override
        public GroupScan getGroupScan (MemoryAdapter adapter) {
            return new Scan (getRowType(adapter));
        }
        
        private class Scan extends BaseScan {

            public Scan (RowType rowType) {
                super (rowType);
            }
            
            @Override
            public Row next() {
                if (rowCounter == 0) {
                    return new ValuesRow (rowType,
                            AkBundle.INSTANCE.id().name(),
                            AkBundle.INSTANCE.id().uuid().toString(),
                            ++rowCounter);
                } else if (rowCounter == 1) {
                    return new ValuesRow (rowType,
                            MBundle.INSTANCE.id().name(),
                            MBundle.INSTANCE.id().uuid().toString(),
                            ++rowCounter);
                }
                return null;
            }
        }
    }
    private class TypeFactory extends BasicFactoryBase {
        public TypeFactory (TableName sourceTable) {
            super(sourceTable);
        }
        
        @Override
        public long rowCount() {
            return Types.types().size() - Types.unsupportedTypes().size();
        }
        
        @Override
        public GroupScan getGroupScan (MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }
        
        private class Scan extends BaseScan {
            final Iterator<Type> typesList;
            public Scan (RowType rowType) {
                super(rowType);
                typesList = Types.types().iterator();
            }
            
            @Override
            public Row next() {
                if (!typesList.hasNext()) 
                    return null;
                
                // Skip the unsupported types
                Type type = null;
                do {
                    if (!typesList.hasNext()) 
                        return null;
                    type = typesList.next();
                } while (Types.unsupportedTypes().contains(type));
                
                boolean indexable = !Types.unsupportedIndexTypes().contains(type);
                TInstance tInstance = Column.generateTInstance(null, type, 1L, 1L, true);
                PostgresType pgType = PostgresType.fromAIS(type, 1L, 1L, false, tInstance);
                
                String bundle = tInstance.typeClass().name().bundleId().name();
                String category = tInstance.typeClass().name().categoryName();
                
                String attribute1 = null;
                String attribute2 = null;
                String attribute3 = null;
                Iterator<? extends Attribute> attrs = tInstance.typeClass().attributes().iterator();
                if (attrs.hasNext()) {
                    attribute1 = attrs.next().name();
                }
                if (attrs.hasNext()) {
                    attribute2 = attrs.next().name();
                }
                if (attrs.hasNext()) {
                    attribute3 = attrs.next().name();
                }
                Long size = type.fixedSize() ? type.maxSizeBytes() : null;
                
                Integer jdbcTypeID = tInstance.dataTypeDescriptor().getJDBCTypeId();
                
                return new ValuesRow (rowType,
                        type.name(),
                        category,
                        bundle,
                        attribute1, 
                        attribute2,
                        attribute3,
                        size,
                        (long)pgType.getOid(),
                        (long)jdbcTypeID,
                        boolResult(indexable),
                        ++rowCounter);
            }
        }
    }
    
    //
    // Package, for testing
    //

    static AkibanInformationSchema createTablesToRegister() {
        // bug1127376: Grouping constraint names are auto-generated and very long. Use a big value until that changes.
        final int GROUPING_CONSTRAINT_MAX = PATH_MAX;

        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.table(SCHEMATA)
                .colString("schema_name", IDENT_MAX, false)
                .colString("schema_owner", IDENT_MAX, true)
                .colString("default_character_set_schema", IDENT_MAX, true)
                .colString("default_character_set_name", IDENT_MAX, true)
                .colString("default_collation_name", IDENT_MAX, true);
        //primary key (schema_name)
        builder.table(TABLES)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("table_type", IDENT_MAX, false)
                .colBigInt("table_id", true)
                .colBigInt("ordinal", true)
                .colString("tree_name", PATH_MAX, true)
                .colString("character_set_schema", IDENT_MAX, true)
                .colString("character_set_name", IDENT_MAX, true)
                .colString("collation_schema", IDENT_MAX, true)
                .colString("collation_name", IDENT_MAX, true);
        //primary key (schema_name, table_name)
        //foreign_key (schema_name) references SCHEMATA (schema_name)
        //foreign key (character_set_schema, character_set_name) references CHARACTER_SETS
        //foreign key (collations_schema, collation_name) references COLLATIONS
        builder.table(COLUMNS)
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
                .colString("character_set_schema", IDENT_MAX, true)
                .colString("character_set_name", IDENT_MAX, true)
                .colString("collation_schema", IDENT_MAX, true)
                .colString("collation_name", IDENT_MAX, true)
                .colString("identity_generation", IDENT_MAX, true)
                .colBigInt("identity_start", true)
                .colBigInt("identity_increment", true)
                .colBigInt("identity_maximum", true)
                .colBigInt("identity_minimum", true)
                .colString("identity_cycle", YES_NO_MAX, true)
                .colString("is_updatable", YES_NO_MAX, true)
                .colString("sequence_schema", IDENT_MAX, true)
                .colString("sequence_name", IDENT_MAX, true);
        //primary key(schema_name, table_name, column_name)
        //foreign key(schema_name, table_name) references TABLES (schema_name, table_name)
        //foreign key (type) references TYPES (type_name)
        //foreign key (character_set_schema, character_set_name) references CHARACTER_SETS
        //foreign key (collation_schema, collation_name) references COLLATIONS
        builder.table(TABLE_CONSTRAINTS)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("constraint_name", GROUPING_CONSTRAINT_MAX, false)
                .colString("constraint_type", DESCRIPTOR_MAX, false)
                .colString("is_deferable", YES_NO_MAX, false)
                .colString("initially_deferred", YES_NO_MAX, false)
                .colString("enforced", YES_NO_MAX, false);
        //primary key (schema_name, table_name, constraint_name)
        //foreign key (schema_name, table_name) references TABLES
        builder.table(REFERENTIAL_CONSTRAINTS)
            .colString("constraint_schema_name", IDENT_MAX, false)
            .colString("constraint_table_name", IDENT_MAX, false)
            .colString("constraint_name", IDENT_MAX, false)
            .colString("unique_schema_name", IDENT_MAX, false)
            .colString("unique_table_name", IDENT_MAX, false)
            .colString("unique_constraint_name", IDENT_MAX, false)
            .colString("update_rule", DESCRIPTOR_MAX, false)
            .colString("delete_rule", DESCRIPTOR_MAX, false);
        //foreign key (schema_name, table_name, constraint_name)
        //    references TABLE_CONSTRAINTS (schema_name, table_name, constraint_name)
        builder.table(GROUPING_CONSTRAINTS) 
                .colString("root_schema_name", IDENT_MAX, false)
                .colString("root_table_name", IDENT_MAX, false)
                .colString("constraint_schema_name", IDENT_MAX, false)
                .colString("constraint_table_name", IDENT_MAX, false)
                .colString("path", IDENT_MAX, false)
                .colBigInt("depth", false)
                .colString("constraint_name", GROUPING_CONSTRAINT_MAX, true)
                .colString("unique_schema_name", IDENT_MAX, true)
                .colString("unique_table_name", IDENT_MAX, true)
                .colString("unique_constraint_name", GROUPING_CONSTRAINT_MAX, true);
        //foreign key (schema_name, table_name, constraint_name)
        //    references TABLE_CONSTRAINTS (schema_name, table_name, constraint_name)
        builder.table(KEY_COLUMN_USAGE)
            .colString("table_schema", IDENT_MAX, false)
            .colString("table_name", IDENT_MAX, false)
            .colString("constraint_name", GROUPING_CONSTRAINT_MAX, false)
            .colString("column_name", IDENT_MAX, false)
            .colBigInt("ordinal_position", false)
            .colBigInt("position_in_unique_constraint", true);
        //primary key  (schema_name, table_name, constraint_name, column_name),
        //foreign key (schema_name, table_name, constraint_name) references TABLE_CONSTRAINTS
        builder.table(INDEXES)
                .colString("schema_name", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("index_name", IDENT_MAX, false)
                .colString("constraint_name", IDENT_MAX, true)
                .colBigInt("index_id", false)
                .colString("tree_name", PATH_MAX, true)
                .colString("index_type", IDENT_MAX, false)
                .colString("is_unique", YES_NO_MAX, false)
                .colString("join_type", DESCRIPTOR_MAX, true)
                .colString("index_method", IDENT_MAX, true);
        //primary key(schema_name, table_name, index_name)
        //foreign key (schema_name, table_name, constraint_name)
        //    references TABLE_CONSTRAINTS (schema_name, table_name, constraint_name)
        //foreign key (schema_name, table_name) references TABLES (schema_name, table_name)
        builder.table(INDEX_COLUMNS)
                .colString("schema_name", IDENT_MAX, false)
                .colString("index_name", IDENT_MAX, false)
                .colString("index_table_name", IDENT_MAX, false)
                .colString("column_schema_name", IDENT_MAX, false)
                .colString("column_table_name", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false)
                .colBigInt("ordinal_position", false)
                .colString("is_ascending", IDENT_MAX, false)
                .colBigInt("indexed_length", true);
        //primary key(schema_name, index_name, index_table_name, column_table_name, column_name)
        //foreign key(schema_name, index_table_name, index_name)
        //    references INDEXES (schema_name, table_name, index_name)
        //foreign key (schema_name, column_table_name, column_name)
        //    references COLUMNS (schema_name, table_name, column_name)
        builder.table(SEQUENCES)
                .colString("sequence_schema", IDENT_MAX, false)
                .colString("sequence_name", IDENT_MAX, false)
                .colString("tree_name", IDENT_MAX, false)
                .colBigInt("start_value", false)
                .colBigInt("increment", false)
                .colBigInt("minimum_value", false)
                .colBigInt("maximum_value", false)
                .colString("cycle_option", YES_NO_MAX, false);
                
        builder.table(VIEWS)
                .colString("schema_name", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colText("view_definition", false)
                .colString("is_updatable", YES_NO_MAX, false);
        //primary key(schema_name, table_name)
        //foreign key(schema_name, table_name) references TABLES (schema_name, table_name)

        builder.table(VIEW_TABLE_USAGE)
                .colString("view_schema", IDENT_MAX, false)
                .colString("view_name", IDENT_MAX, false)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false);
        //foreign key(view_schema, view_name) references VIEWS (schema_name, table_name)
        //foreign key(table_schema, table_name) references TABLES (schema_name, table_name)

        builder.table(VIEW_COLUMN_USAGE)
                .colString("view_schema", IDENT_MAX, false)
                .colString("view_name", IDENT_MAX, false)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false);
        //foreign key(view_schema, view_name) references VIEWS (schema_name, table_name)
        //foreign key(table_schema, table_name) references TABLES (schema_name, table_name)

        builder.table(ROUTINES)
                .colString("routine_schema", IDENT_MAX, false)
                .colString("routine_name", IDENT_MAX, false)
                .colString("routine_type", IDENT_MAX, false)
                .colText("routine_definition", true)
                .colString("external_name", PATH_MAX, true)
                .colString("language", IDENT_MAX, false)
                .colString("calling_convention", IDENT_MAX, false)
                .colString("is_deterministic", YES_NO_MAX, false)
                .colString("sql_data_access", IDENT_MAX, true)
                .colString("is_null_call", YES_NO_MAX, false)
                .colBigInt("max_dynamic_result_sets", false);
        //primary key(routine_schema, routine_name)

        builder.table(PARAMETERS)
                .colString("routine_schema", IDENT_MAX, false)
                .colString("routine_name", IDENT_MAX, false)
                .colString("parameter_name", IDENT_MAX, true)
                .colBigInt("position", false)
                .colString("type", 32, false)
                .colBigInt("length", true)
                .colBigInt("precision", true)
                .colBigInt("scale", true)
                .colString("parameter_mode", IDENT_MAX, false)
                .colString("is_result", YES_NO_MAX, false);
        //primary key(routine_schema, routine_name, parameter_name)
        //foreign key(routine_schema, routine_name) references ROUTINES (routine_schema, routine_name)
        //foreign key (type) references TYPES (type_name)

        builder.table(JARS)
                .colString("jar_schema", IDENT_MAX, false)
                .colString("jar_name", IDENT_MAX, false)
                .colString("java_path", PATH_MAX, true);
        //primary key(jar_schema, jar_name)

        builder.table(ROUTINE_JAR_USAGE)
                .colString("routine_schema", IDENT_MAX, false)
                .colString("routine_name", IDENT_MAX, false)
                .colString("jar_schema", IDENT_MAX, false)
                .colString("jar_name", IDENT_MAX, false);
        //foreign key(routine_schema, routine_name) references ROUTINES (routine_schema, routine_name)
        //foreign key(jar_schema, jar_name) references JARS (jar_schema, jar_name)

        builder.table(SCRIPT_ENGINES)
                .colLong("engine_id", false)
                .colString("engine_name", IDENT_MAX, false)
                .colString("engine_version", IDENT_MAX, false)
                .colString("language_name", IDENT_MAX, false)
                .colString("language_version", IDENT_MAX, false);
        //primary key(engine_id)

        builder.table(SCRIPT_ENGINE_NAMES)
                .colString("name", IDENT_MAX, false)
                .colString("engine_id", IDENT_MAX, false);
        //foreign key (engine_id) references SCRIPT_ENGINES (engine_id)

        builder.table(TYPES)
            .colString("type_name", IDENT_MAX, false)
            .colString("type_category", IDENT_MAX, false)
            .colString("type_bundle_name", IDENT_MAX)
            .colString("attribute_1", IDENT_MAX)
            .colString("attribute_2", IDENT_MAX)
            .colString("attribute_3", IDENT_MAX)
            .colLong  ("fixed_length")
            .colLong("postgres_oid")
            .colLong("jdbc_type_id")
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
