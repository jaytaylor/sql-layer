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
import com.akiban.ais.model.CharsetAndCollation;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.Parameter;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.View;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.memoryadapter.BasicFactoryBase;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryGroupCursor;
import com.akiban.qp.memoryadapter.MemoryGroupCursor.GroupScan;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.Service;
import com.akiban.server.store.AisHolder;
import com.akiban.server.store.SchemaManager;
import com.google.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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

    private static final String CHARSET_SCHEMA = SCHEMA_NAME;
    private static final String COLLATION_SCHEMA = SCHEMA_NAME;

    private final AisHolder aisHolder;
    
    @Inject
    public BasicInfoSchemaTablesServiceImpl(AisHolder aisHolder, SchemaManager schemaManager) {
        super(schemaManager);
        this.aisHolder = aisHolder;
    }

    @Override
    public void start() {
        AkibanInformationSchema ais = createTablesToRegister();
        attachFactories(ais, true);
    }

    @Override
    public void stop() {
        // Nothing
    }

    @Override
    public void crash() {
        // Nothing
    }

    private class SchemataFactory extends BasicFactoryBase {
        public SchemataFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getSchemas().size();
        }

        private class Scan extends BaseScan {
            final Iterator<Schema> it = aisHolder.getAis().getSchemas().values().iterator();

            public Scan(RowType rowType) {
                super (rowType);
            }

            @Override
            public Row next() {
                if(!it.hasNext()) {
                    return null;
                }
                Schema schema = it.next();
                return new ValuesRow(rowType,
                                     schema.getName(),
                                     null, // owner
                                     null, // charset
                                     null, // collation
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class TablesFactory extends BasicFactoryBase {
        public TablesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getUserTables().size() +
                aisHolder.getAis().getViews().size() ;
        }

        private class Scan extends BaseScan {
            final Iterator<UserTable> tableIt = aisHolder.getAis().getUserTables().values().iterator();
            Iterator<View> viewIt = null;

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if(viewIt == null) {
                    if(tableIt.hasNext()) {
                        UserTable table = tableIt.next();
                        final String tableType = table.hasMemoryTableFactory() ? "DICTIONARY VIEW" : "TABLE";
                        return new ValuesRow(rowType,
                                             table.getName().getSchemaName(),
                                             table.getName().getTableName(),
                                             tableType,
                                             table.getTableId(),
                                             table.hasMemoryTableFactory() ? null : table.getGroup().getTreeName(),
                                             CHARSET_SCHEMA,
                                             table.getCharsetAndCollation().charset(),
                                             COLLATION_SCHEMA,
                                             table.getCharsetAndCollation().collation(),
                                             ++rowCounter /*hidden pk*/);
                    } else {
                        viewIt = aisHolder.getAis().getViews().values().iterator();
                    }
                }
                if(viewIt.hasNext()) {
                    View view = viewIt.next();
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
                                         ++rowCounter /*hidden pk*/);
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
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for(UserTable table : aisHolder.getAis().getUserTables().values()) {
                count += table.getColumns().size();
            }
            for(View view : aisHolder.getAis().getViews().values()) {
                count += view.getColumns().size();
            }
            return count;
        }
        
        private class Scan extends BaseScan {
            final Iterator<UserTable> tableIt = aisHolder.getAis().getUserTables().values().iterator();
            Iterator<View> viewIt = null;
            Iterator<Column> columnIt;

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                while(columnIt == null || !columnIt.hasNext()) {
                    if(viewIt == null) {
                        if(tableIt.hasNext()) {
                            columnIt = tableIt.next().getColumns().iterator();
                            continue;
                        } else {
                            viewIt = aisHolder.getAis().getViews().values().iterator();
                        }
                    }
                    if(viewIt.hasNext()) {
                        columnIt = viewIt.next().getColumns().iterator();
                    } else {
                        return null;
                    }
                }

                Column column = columnIt.next();
                final Long length;
                if(column.getType().fixedSize()) {
                    length = column.getMaxStorageSize();
                } else {
                    length = column.getTypeParameter1();
                }

                // TODO: This should come from type attributes when new types go in
                Integer precision = null;
                Integer scale = null;
                CharsetAndCollation charAndColl = null;
                switch(column.getType().akType()) {
                    case DECIMAL:
                        precision = column.getTypeParameter1().intValue();
                        scale = column.getTypeParameter2().intValue();
                    break;
                    case VARCHAR:
                    case TEXT:
                        charAndColl = column.getCharsetAndCollation();
                    break;
                }
                
                String sequenceSchema = null;
                String sequenceName = null;
                String identityGeneration = null;
                if (column.getIdentityGenerator() != null) {
                    sequenceSchema = column.getIdentityGenerator().getSequenceName().getSchemaName();
                    sequenceName   = column.getIdentityGenerator().getSequenceName().getTableName();
                    identityGeneration = column.getDefaultIdentity() ? "BY DEFAULT" : "ALWAYS";
                }
                        
                
                return new ValuesRow(rowType,
                                     column.getColumnar().getName().getSchemaName(),
                                     column.getColumnar().getName().getTableName(),
                                     column.getName(),
                                     column.getPosition(),
                                     column.getType().name(),
                                     boolResult(column.getNullable()),
                                     length,
                                     precision,
                                     scale,
                                     column.getPrefixSize(),
                                     column.getInitialAutoIncrementValue(),
                                     charAndColl != null ? CHARSET_SCHEMA : null,
                                     charAndColl != null ? charAndColl.charset() : null,
                                     charAndColl != null ? COLLATION_SCHEMA : null,
                                     charAndColl != null ? charAndColl.collation() : null,
                                     sequenceSchema,
                                     sequenceName,
                                     identityGeneration,
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class TableConstraintsFactory extends BasicFactoryBase {
        public TableConstraintsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private TableConstraintsIteration newIteration() {
            return new TableConstraintsIteration(aisHolder.getAis().getUserTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            TableConstraintsIteration it = newIteration();
            while(it.next()) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final TableConstraintsIteration it = newIteration();

            public Scan(RowType rowType) {
                super(rowType);
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
        final UserTable root;
        final String path;
        final UserTable table;

        public RootPathTable(UserTable root, String path, UserTable table) {
            this.root = root;
            this.path = path;
            this.table = table;
        }

        @Override
        public String toString() {
            return root.getName() + ", " + table.getName() + ", " + path;
        }
    }

    private static class TableIDComparator implements Comparator<UserTable> {
        @Override
        public int compare(UserTable o1, UserTable o2) {
            return o1.getTableId().compareTo(o2.getTableId());
        }
    }
    private static final TableIDComparator TABLE_ID_COMPARATOR = new TableIDComparator();

    private static void addBranchToList(List<RootPathTable> list, StringBuilder builder, UserTable root, UserTable branch) {
        if(builder.length() != 0) {
            builder.append('/');
        }
        builder.append(branch.getName().getSchemaName());
        builder.append('.');
        builder.append(branch.getName().getTableName());
        list.add(new RootPathTable(root, builder.toString(), branch));

        // For tables at the same depth, comparing table IDs is currently synonymous with ordinals
        List<UserTable> children = new ArrayList<UserTable>();
        for(Join join : branch.getChildJoins()) {
            children.add(join.getChild());
        }
        Collections.sort(children, TABLE_ID_COMPARATOR);

        for(UserTable child : children) {
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
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getUserTables().values().size();
        }

        private class Scan extends BaseScan {
            final List<RootPathTable> rootPathTables;
            final Iterator<RootPathTable> it;

            public Scan(RowType rowType) {
                super(rowType);

                // Desired output: groups together, ordered by branch (ordinal), then ordered by depth
                // Highest level sorting will be by schema.root, which seems as good as any
                rootPathTables = new ArrayList<RootPathTable>();
                Collection<UserTable> allTables = aisHolder.getAis().getUserTables().values();
                StringBuilder builder = new StringBuilder();
                for(UserTable table : allTables) {
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
                UserTable table = rpt.table;
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
                                     table.getDepth(),                  // depth
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

        private TableConstraintsIteration newIteration() {
            return new TableConstraintsIteration(aisHolder.getAis().getUserTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            int count = 0;
            TableConstraintsIteration it = newIteration();
            while(it.next()) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final TableConstraintsIteration it = newIteration();
            Iterator<IndexColumn> indexColIt;
            Iterator<JoinColumn> joinColIt;
            String colName;
            int colPos;
            Integer posInUnique;

            public Scan(RowType rowType) {
                super(rowType);
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
                    posInUnique = findPosInIndex(joinColumn.getParent(), joinColumn.getParent().getUserTable().getPrimaryKey().getIndex());
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

        private IndexIteration newIteration() {
            return new IndexIteration(aisHolder.getAis().getUserTables().values().iterator());
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            IndexIteration it = newIteration();
            while(it.next() != null) {
                ++count;
            }
            return count;
        }

        private class Scan extends BaseScan {
            final IndexIteration indexIt = newIteration();

            public Scan(RowType rowType) {
                super(rowType);
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
                                     indexIt.curTable.hasMemoryTableFactory() ? null : index.getTreeName(),
                                     indexType,
                                     boolResult(index.isUnique()),
                                     index.isGroupIndex() ? index.getJoinType().name() : null,
                                     index.isSpatial() ? index.getIndexMethod().name() : null,
                                     ++rowCounter /*hidden pk*/);
            }
        }
    }

    private class IndexColumnsFactory extends BasicFactoryBase {
        public IndexColumnsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        private IndexIteration newIteration() {
            return new IndexIteration(aisHolder.getAis().getUserTables().values().iterator());
        }

        @Override
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            IndexIteration indexIt = newIteration();
            long count = 0;
            Index index;
            while((index = indexIt.next()) != null) {
                count += index.getKeyColumns().size();
            }
            return count;
        }

        private class Scan extends BaseScan {
            final IndexIteration indexIt = newIteration();
            Iterator<IndexColumn> indexColumnIt;

            public Scan(RowType rowType) {
                super(rowType);
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
                                     indexColumn.getPosition(),
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
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getSequences().size();
        }
        
        private class Scan extends BaseScan {
            final Iterator<Sequence> it = aisHolder.getAis().getSequences().values().iterator();
            
            public Scan (RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if (it.hasNext()) {
                    Sequence sequence  = it.next();
                    return new ValuesRow (rowType,
                            sequence.getSequenceName().getSchemaName(),
                            sequence.getSequenceName().getTableName(),
                            sequence.getTreeName(),
                            sequence.getStartsWith(),
                            sequence.getIncrement(),
                            sequence.getMinValue(),
                            sequence.getMaxValue(),
                            boolResult(sequence.isCycle()),
                            ++rowCounter);
                } else {
                    return null;
                }
            }
        }
    }
    private class ViewsFactory extends BasicFactoryBase {
        public ViewsFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getViews().size() ;
        }

        private class Scan extends BaseScan {
            final Iterator<View> it = aisHolder.getAis().getViews().values().iterator();

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if(it.hasNext()) {
                    View view = it.next();
                    return new ValuesRow(rowType,
                                         view.getName().getSchemaName(),
                                         view.getName().getTableName(),
                                         view.getDefinition(),
                                         boolResult(false),
                                         ++rowCounter /*hidden pk*/);
                } else {
                    return null;
                }
            }
        }
    }

    private class ViewTableUsageFactory extends BasicFactoryBase {
        public ViewTableUsageFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (View view : aisHolder.getAis().getViews().values()) {
                count += view.getTableReferences().size();
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Iterator<View> viewIt = aisHolder.getAis().getViews().values().iterator();
            View view;
            Iterator<TableName> tableIt = null;

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                while((tableIt == null) || !tableIt.hasNext()) {
                    if (!viewIt.hasNext())
                        return null;
                    view = viewIt.next();
                    tableIt = view.getTableReferences().iterator();
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
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (View view : aisHolder.getAis().getViews().values()) {
                for (Collection<String> columns : view.getTableColumnReferences().values()) {
                    count += columns.size();
                }
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Iterator<View> viewIt = aisHolder.getAis().getViews().values().iterator();
            View view;
            Iterator<Map.Entry<TableName,Collection<String>>> tableIt = null;
            TableName table;
            Iterator<String> columnIt = null;

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                while((columnIt == null) || !columnIt.hasNext()) {
                    while((tableIt == null) || !tableIt.hasNext()) {
                        if (!viewIt.hasNext())
                            return null;
                        view = viewIt.next();
                        tableIt = view.getTableColumnReferences().entrySet().iterator();
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
    
    private static class TableConstraintsIteration {
        private final Iterator<UserTable> tableIt;
        private Iterator<? extends Index> indexIt;
        private UserTable curTable;
        private Index curIndex;
        private String name;
        private String type;

        public TableConstraintsIteration(Iterator<UserTable> tableIt) {
            this.tableIt = tableIt;
        }

        public boolean next() {
            while(curTable != null || tableIt.hasNext()) {
                if(curTable == null) {
                    curTable = tableIt.next();
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

        public UserTable getTable() {
            return curTable;
        }

        public Index getIndex() {
            return curIndex;
        }

        public boolean isGrouping() {
            return indexIt == null;
        }
    }

    private static class IndexIteration {
        private final Iterator<UserTable> tableIt;
        Iterator<TableIndex> tableIndexIt;
        Iterator<GroupIndex> groupIndexIt;
        UserTable curTable;

        public IndexIteration(Iterator<UserTable> tableIt) {
            this.tableIt = tableIt;
        }

        Index next() {
            while(tableIndexIt == null || !tableIndexIt.hasNext()) {
                while(groupIndexIt != null && groupIndexIt.hasNext()) {
                    GroupIndex index = groupIndexIt.next();
                    if(index.leafMostTable() == curTable) {
                        return index;
                    }
                }
                if(tableIt.hasNext()) {
                    curTable = tableIt.next();
                    tableIndexIt = curTable.getIndexes().iterator();
                    groupIndexIt = curTable.getGroup().getIndexes().iterator();
                } else {
                    return null;
                }
            }
            return tableIndexIt.next();
        }

        UserTable getTable() {
            return curTable;
        }
    }

    private class RoutinesFactory extends BasicFactoryBase {
        public RoutinesFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getRoutines().size() ;
        }

        private class Scan extends BaseScan {
            final Iterator<Routine> it = aisHolder.getAis().getRoutines().values().iterator();

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if(it.hasNext()) {
                    Routine routine = it.next();
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
                                         routine.getDynamicResultSets(),
                                         ++rowCounter /*hidden pk*/);
                } else {
                    return null;
                }
            }
        }
    }

    private class ParametersFactory extends BasicFactoryBase {
        public ParametersFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (Routine routine : aisHolder.getAis().getRoutines().values()) {
                if (routine.getReturnValue() != null) {
                    count++;
                }
                count += routine.getParameters().size();
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Iterator<Routine> routinesIt = aisHolder.getAis().getRoutines().values().iterator();
            Iterator<Parameter> paramsIt;
            long ordinal;

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                Parameter param = null;
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
                switch(param.getType().akType()) {
                    case DECIMAL:
                        precision = param.getTypeParameter1();
                        scale = param.getTypeParameter2();
                    break;
                    case VARCHAR:
                        length = param.getTypeParameter1();
                    break;
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
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return aisHolder.getAis().getSQLJJars().size() ;
        }

        private class Scan extends BaseScan {
            final Iterator<SQLJJar> it = aisHolder.getAis().getSQLJJars().values().iterator();

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                if(it.hasNext()) {
                    SQLJJar jar = it.next();
                    return new ValuesRow(rowType,
                                         jar.getName().getSchemaName(),
                                         jar.getName().getTableName(),
                                         jar.getURL().toExternalForm(),
                                         ++rowCounter /*hidden pk*/);
                } else {
                    return null;
                }
            }
        }
    }

    private class RoutineJarUsageFactory extends BasicFactoryBase {
        public RoutineJarUsageFactory(TableName sourceTable) {
            super(sourceTable);
        }

        @Override
        public GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            long count = 0;
            for (Routine routine : aisHolder.getAis().getRoutines().values()) {
                if (routine.getSQLJJar() != null) {
                    count++;
                }
            }
            return count;
        }

        private class Scan extends BaseScan {
            final Iterator<Routine> it = aisHolder.getAis().getRoutines().values().iterator();

            public Scan(RowType rowType) {
                super(rowType);
            }

            @Override
            public Row next() {
                while (it.hasNext()) {
                    Routine routine = it.next();
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

    //
    // Package, for testing
    //

    static AkibanInformationSchema createTablesToRegister() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(SCHEMATA)
                .colString("schema_name", IDENT_MAX, false)
                .colString("schema_owner", IDENT_MAX, true)
                .colString("default_character_set_name", IDENT_MAX, true)
                .colString("default_collation_name", IDENT_MAX, true);
        //primary key (schema_name)
        builder.userTable(TABLES)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("table_type", IDENT_MAX, false)
                .colBigInt("table_id", true)
                .colString("tree_name", PATH_MAX, true)
                .colString("character_set_schema", IDENT_MAX, true)
                .colString("character_set_name", IDENT_MAX, true)
                .colString("collation_schema", IDENT_MAX, true)
                .colString("collation_name", IDENT_MAX, true);
        //primary key (schema_name, table_name)
        //foreign_key (schema_name) references SCHEMATA (schema_name)
        //foreign key (character_set_schema, character_set_name) references CHARACTER_SETS
        //foreign key (collations_schema, collation_name) references COLLATIONS
        builder.userTable(COLUMNS)
                .colString("schema_name", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false)
                .colBigInt("position", false)
                .colString("type", 32, false)
                .colString("nullable", 3, false)
                .colBigInt("length", false)
                .colBigInt("precision", true)
                .colBigInt("scale", true)
                .colBigInt("prefix_size", true)
                .colBigInt("identity_start", true)
                .colString("character_set_schema", IDENT_MAX, true)
                .colString("character_set_name", IDENT_MAX, true)
                .colString("collation_schema", IDENT_MAX, true)
                .colString("collation_name", IDENT_MAX, true)
                .colString("sequence_schema", IDENT_MAX, true)
                .colString("sequence_name", IDENT_MAX, true)
                .colString("identity_generation", IDENT_MAX, true);
        //primary key(schema_name, table_name, column_name)
        //foreign key(schema_name, table_name) references TABLES (schema_name, table_name)
        //foreign key (type) references TYPES (type_name)
        //foreign key (character_set_schema, character_set_name) references CHARACTER_SETS
        //foreign key (collation_schema, collation_name) references COLLATIONS
        builder.userTable(TABLE_CONSTRAINTS)
                .colString("schema_name", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("constraint_name", IDENT_MAX, false)
                .colString("constraint_type", 32, false);
        //primary key (schema_name, table_name, constraint_name)
        //foreign key (schema_name, table_name) references TABLES
        builder.userTable(REFERENTIAL_CONSTRAINTS)
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
        builder.userTable(GROUPING_CONSTRAINTS) 
                .colString("root_schema_name", IDENT_MAX, false)
                .colString("root_table_name", IDENT_MAX, false)
                .colString("constraint_schema_name", IDENT_MAX, false)
                .colString("constraint_table_name", IDENT_MAX, false)
                .colString("path", IDENT_MAX, false)
                .colBigInt("depth", false)
                .colString("constraint_name", IDENT_MAX, true)
                .colString("unique_schema_name", IDENT_MAX, true)
                .colString("unique_table_name", IDENT_MAX, true)
                .colString("unique_constraint_name", IDENT_MAX, true);                            
        //foreign key (schema_name, table_name, constraint_name)
        //    references TABLE_CONSTRAINTS (schema_name, table_name, constraint_name)
        builder.userTable(KEY_COLUMN_USAGE)
            .colString("schema_name", IDENT_MAX, false)
            .colString("table_name", IDENT_MAX, false)
            .colString("constraint_name", IDENT_MAX, false)
            .colString("column_name", IDENT_MAX, false)
            .colBigInt("ordinal_position", false)
            .colBigInt("position_in_unique_constraint", true);
        //primary key  (schema_name, table_name, constraint_name, column_name),
        //foreign key (schema_name, table_name, constraint_name) references TABLE_CONSTRAINTS
        builder.userTable(INDEXES)
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
        builder.userTable(INDEX_COLUMNS)
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
        builder.userTable(SEQUENCES)
                .colString("sequence_schema", IDENT_MAX, false)
                .colString("sequence_name", IDENT_MAX, false)
                .colString("tree_name", IDENT_MAX, false)
                .colBigInt("start_value", false)
                .colBigInt("increment", false)
                .colBigInt("minimum_value", false)
                .colBigInt("maximum_value", false)
                .colString("cycle_option", YES_NO_MAX, false);
                
        builder.userTable(VIEWS)
                .colString("schema_name", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colText("view_definition", false)
                .colString("is_updatable", YES_NO_MAX, false);
        //primary key(schema_name, table_name)
        //foreign key(schema_name, table_name) references TABLES (schema_name, table_name)

        builder.userTable(VIEW_TABLE_USAGE)
                .colString("view_schema", IDENT_MAX, false)
                .colString("view_name", IDENT_MAX, false)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false);
        //foreign key(view_schema, view_name) references VIEWS (schema_name, table_name)
        //foreign key(table_schema, table_name) references TABLES (schema_name, table_name)

        builder.userTable(VIEW_COLUMN_USAGE)
                .colString("view_schema", IDENT_MAX, false)
                .colString("view_name", IDENT_MAX, false)
                .colString("table_schema", IDENT_MAX, false)
                .colString("table_name", IDENT_MAX, false)
                .colString("column_name", IDENT_MAX, false);
        //foreign key(view_schema, view_name) references VIEWS (schema_name, table_name)
        //foreign key(table_schema, table_name) references TABLES (schema_name, table_name)

        builder.userTable(ROUTINES)
                .colString("routine_schema", IDENT_MAX, false)
                .colString("routine_name", IDENT_MAX, false)
                .colString("routine_type", IDENT_MAX, false)
                .colText("routine_definition", true)
                .colString("external_name", PATH_MAX, true)
                .colString("language", IDENT_MAX, false)
                .colString("calling_convention", IDENT_MAX, false)
                .colString("is_deterministic", YES_NO_MAX, false)
                .colString("sql_data_access", IDENT_MAX, false)
                .colString("is_null_call", YES_NO_MAX, false)
                .colBigInt("max_dynamic_result_sets", false);
        //primary key(routine_schema, routine_name)

        builder.userTable(PARAMETERS)
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

        builder.userTable(JARS)
                .colString("jar_schema", IDENT_MAX, false)
                .colString("jar_name", IDENT_MAX, false)
                .colString("java_path", PATH_MAX, true);
        //primary key(jar_schema, jar_name)

        builder.userTable(ROUTINE_JAR_USAGE)
                .colString("routine_schema", IDENT_MAX, false)
                .colString("routine_name", IDENT_MAX, false)
                .colString("jar_schema", IDENT_MAX, false)
                .colString("jar_name", IDENT_MAX, false);
        //foreign key(routine_schema, routine_name) references ROUTINES (routine_schema, routine_name)
        //foreign key(jar_schema, jar_name) references JARS (jar_schema, jar_name)

        return builder.ais(false);
    }

    void attachFactories(AkibanInformationSchema ais, boolean doRegister) {
        attach(ais, doRegister, SCHEMATA, SchemataFactory.class);
        attach(ais, doRegister, TABLES, TablesFactory.class);
        attach(ais, doRegister, COLUMNS, ColumnsFactory.class);
        attach(ais, doRegister, TABLE_CONSTRAINTS, TableConstraintsFactory.class);
        attach(ais, doRegister, REFERENTIAL_CONSTRAINTS, ReferentialConstraintsFactory.class);
        attach(ais, doRegister, GROUPING_CONSTRAINTS, GroupingConstraintsFactory.class);
        attach(ais, doRegister, KEY_COLUMN_USAGE, KeyColumnUsageFactory.class);
        attach(ais, doRegister, INDEXES, IndexesFactory.class);
        attach(ais, doRegister, INDEX_COLUMNS, IndexColumnsFactory.class);
        attach(ais, doRegister, SEQUENCES, SequencesFactory.class);
        attach(ais, doRegister, VIEWS, ViewsFactory.class);
        attach(ais, doRegister, VIEW_TABLE_USAGE, ViewTableUsageFactory.class);
        attach(ais, doRegister, VIEW_COLUMN_USAGE, ViewColumnUsageFactory.class);
        attach(ais, doRegister, ROUTINES, RoutinesFactory.class);
        attach(ais, doRegister, PARAMETERS, ParametersFactory.class);
        attach(ais, doRegister, JARS, JarsFactory.class);
        attach(ais, doRegister, ROUTINE_JAR_USAGE, RoutineJarUsageFactory.class);
    }
}
