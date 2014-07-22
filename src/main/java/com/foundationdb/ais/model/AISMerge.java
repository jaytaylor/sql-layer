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

package com.foundationdb.ais.model;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.server.error.DuplicateIndexException;
import com.foundationdb.server.error.IndexLacksColumnsException;
import com.foundationdb.server.error.JoinToMultipleParentsException;
import com.foundationdb.server.error.JoinToUnknownTableException;
import com.foundationdb.server.error.JoinToWrongColumnsException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchGroupException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedIndexException;
import com.foundationdb.server.error.TableNotInGroupException;
import com.foundationdb.server.store.format.StorageFormatRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AISMerge is designed to merge a single Table definition into an existing AIS. The merge process 
 * does not assume that Table.getAIS() returns a validated and complete 
 * AkibanInformationSchema object. 
 * 
 * AISMerge makes a copy of the primaryAIS (from the constructor) before performing the merge process. 
 * The final results is this copies AIS, plus new table, with the full AISValidations suite run, and 
 * frozen. If you pass a frozen AIS into the merge, the copy process unfreeze the copy.
 */
public class AISMerge {
    public enum MergeType { ADD_TABLE, MODIFY_TABLE, ADD_INDEX, OTHER }

    private static class JoinChange {
        public final Join join;
        public final TableName newParentName;
        public final Map<String,String> parentCols;
        public final TableName newChildName;
        public final Map<String,String> childCols;
        public final boolean isNewGroup;

        private JoinChange(Join join, TableName newParentName, Map<String, String> parentCols,
                           TableName newChildName, Map<String, String> childCols, boolean isNewGroup) {
            this.join = join;
            this.newParentName = newParentName;
            this.parentCols = parentCols;
            this.newChildName = newChildName;
            this.childCols = childCols;
            this.isNewGroup = isNewGroup;
        }
    }

    private static class IndexInfo {
        public final Integer id;
        public final StorageDescription storage;

        private IndexInfo(Integer id, StorageDescription storage) {
            this.id = id;
            this.storage = storage;
        }
    }

    private static class IdentityInfo {
        public final TableName tableName;
        public final String columnName;
        public final boolean defaultIdentity;
        public final Sequence sequence;

        public IdentityInfo(TableName tableName, String columnName, boolean defaultIdentity, Sequence sequence) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.defaultIdentity = defaultIdentity;
            this.sequence = sequence;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(AISMerge.class);

    /* state */
    private final AISCloner aisCloner;
    private final AkibanInformationSchema targetAIS;
    private final Table sourceTable;
    private final NameGenerator nameGenerator;
    private final MergeType mergeType;
    private final List<JoinChange> changedJoins;
    private final Map<IndexName,IndexInfo> indexesToFix;
    private final List<IdentityInfo> identityToFix;
    private final Set<TableName> groupsToClear;

    /** Legacy test constructor. Creates an AISMerge for adding a table with a new {@link DefaultNameGenerator}. */
    AISMerge(AISCloner aisCloner, AkibanInformationSchema sourceAIS, Table newTable) {
        this(aisCloner, new DefaultNameGenerator(sourceAIS), copyAISForAdd(aisCloner, sourceAIS), newTable, MergeType.ADD_TABLE, null, null, null, null);
    }

    /** Create a new AISMerge to be used for adding a new table. */
    public static AISMerge newForAddTable(AISCloner aisCloner, NameGenerator generator, AkibanInformationSchema sourceAIS, Table newTable) {
        return new AISMerge(aisCloner, generator, copyAISForAdd(aisCloner, sourceAIS), newTable, MergeType.ADD_TABLE, null, null, null, null);
    }

    /** Create a new AISMerge to be used for modifying a table. */
    public static AISMerge newForModifyTable(AISCloner aisCloner, NameGenerator generator, AkibanInformationSchema sourceAIS,
                                             Collection<ChangedTableDescription> alteredTables) {
        List<JoinChange> changedJoins = new ArrayList<>();
        Map<IndexName,IndexInfo> indexesToFix = new HashMap<>();
        List<IdentityInfo> identityToFix = new ArrayList<>();
        Set<TableName> groupsToClear = new HashSet<>();
        AkibanInformationSchema targetAIS = copyAISForModify(aisCloner, sourceAIS, indexesToFix, changedJoins, identityToFix, alteredTables, groupsToClear);
        return new AISMerge(aisCloner, generator, targetAIS, null, MergeType.MODIFY_TABLE, changedJoins, indexesToFix, identityToFix, groupsToClear);
    }

    /** Create a new AISMerge to be used for adding one, or more, index to a table. Also see {@link #mergeIndex(Index)}. */
    public static AISMerge newForAddIndex(AISCloner aisCloner, NameGenerator generator, AkibanInformationSchema sourceAIS) {
        return new AISMerge(aisCloner, generator, copyAISForAdd(aisCloner, sourceAIS), null, MergeType.ADD_INDEX, null, null, null, null);
    }

    public static AISMerge newForOther(AISCloner aisCloner, NameGenerator generator, AkibanInformationSchema sourceAIS) {
        return new AISMerge(aisCloner, generator, copyAISForAdd(aisCloner, sourceAIS), null, MergeType.OTHER, null, null, null, null);
    }

    private AISMerge(AISCloner aisCloner, NameGenerator nameGenerator, AkibanInformationSchema targetAIS, Table sourceTable,
                     MergeType mergeType, List<JoinChange> changedJoins, Map<IndexName,IndexInfo> indexesToFix,
                     List<IdentityInfo> identityToFix, Set<TableName> groupsToClear) {
        this.aisCloner = aisCloner;
        this.nameGenerator = nameGenerator;
        this.targetAIS = targetAIS;
        this.sourceTable = sourceTable;
        this.mergeType = mergeType;
        this.changedJoins = changedJoins;
        this.indexesToFix = indexesToFix;
        this.identityToFix = identityToFix;
        this.groupsToClear = groupsToClear;
    }


    public static AkibanInformationSchema copyAISForAdd(AISCloner aisCloner, AkibanInformationSchema oldAIS) {
        return aisCloner.clone(oldAIS);
    }

    private static AkibanInformationSchema copyAISForModify(AISCloner aisCloner,
                                                            AkibanInformationSchema oldAIS,
                                                            Map<IndexName,IndexInfo> indexesToFix,
                                                            final List<JoinChange> joinsToFix,
                                                            List<IdentityInfo> identityToFix,
                                                            Collection<ChangedTableDescription> changedTables,
                                                            Set<TableName> groupsToClear)
    {
        final Set<Sequence> excludedSequences = new HashSet<>();
        final Set<Group> excludedGroups = new HashSet<>();
        final Map<TableName,Table> filteredTables = new HashMap<>();
        for(ChangedTableDescription desc : changedTables) {
            // Copy tree names and IDs for pre-existing table and it's indexes
            Table oldTable = oldAIS.getTable(desc.getOldName());
            Table newTable = desc.getNewDefinition();

            // These don't affect final outcome and may be reset later. Needed by clone process.
            if((newTable != null) && (newTable.getGroup() != null)) {
                newTable.setOrdinal(oldTable.getOrdinal());
                newTable.setTableId(oldTable.getTableId());
            }

            // If we're changing table rows *at all*, generate new group tree(s)
            if(desc.isPKAffected() || desc.isTableAffected()) {
                groupsToClear.add(oldTable.getGroup().getName());
                if(newTable != null && newTable.getGroup() != null) {
                    groupsToClear.add(newTable.getGroup().getName());
                }
            }

            switch(desc.getParentChange()) {
                case NONE:
                    // None: Handled by cloning process
                break;
                case META:
                case UPDATE: {
                    Join join = (newTable != null) ? newTable.getParentJoin() : oldTable.getParentJoin();
                    joinsToFix.add(new JoinChange(join, desc.getParentName(), desc.getParentColNames(),
                                                  desc.getNewName(), desc.getColNames(), false));
                } break;
                case ADD:
                    if(newTable == null) {
                        throw new IllegalArgumentException("Invalid change description: " + desc);
                    }
                    joinsToFix.add(new JoinChange(null, null, desc.getParentColNames(),
                                                  desc.getNewName(), desc.getColNames(), false));
                break;
                case DROP: {
                    final Join join;
                    if(newTable != null) {
                        join = newTable.getParentJoin();
                        excludedGroups.add(newTable.getGroup());
                    } else {
                        join = oldTable.getParentJoin();
                    }
                    joinsToFix.add(new JoinChange(join, null, desc.getParentColNames(),
                                                  desc.getNewName(), desc.getColNames(), true));
                } break;
                default:
                    throw new IllegalStateException("Unhandled GroupChange: " + desc.getParentChange());
            }

            Table indexSearchTable = newTable;
            if(newTable == null) {
                indexSearchTable = oldTable;
            } else {
                filteredTables.put(desc.getOldName(), newTable);
            }

            for(Index newIndex : indexSearchTable.getIndexesIncludingInternal()) {
                String oldName = desc.getPreserveIndexes().get(newIndex.getIndexName().getName());
                Index oldIndex = (oldName != null) ? oldTable.getIndexIncludingInternal(oldName) : null;
                if(oldIndex != null) {
                    indexesToFix.put(newIndex.getIndexName(), new IndexInfo(oldIndex.getIndexId(), oldIndex.getStorageDescription()));
                } else {
                    indexesToFix.put(newIndex.getIndexName(), new IndexInfo(null, null));
                }
            }

            for(TableName name : desc.getDroppedSequences()) {
                excludedSequences.add(oldAIS.getSequence(name));
            }

            for(String name : desc.getIdentityAdded()) {
                Column col = newTable.getColumn(name);
                identityToFix.add(new IdentityInfo(desc.getNewName(), name, col.getDefaultIdentity(), col.getIdentityGenerator()));
            }
        }

        return aisCloner.clone(
                oldAIS,
                new ProtobufWriter.TableFilterSelector() {
                    @Override
                    public Columnar getSelected(Columnar columnar) {
                        if(columnar.isTable()) {
                            Columnar filtered = filteredTables.get(columnar.getName());
                            if(filtered != null) {
                                return filtered;
                            }
                        }
                        return columnar;
                    }

                    @Override
                    public boolean isSelected(Group group) {
                        return !excludedGroups.contains(group);
                    }

                    @Override
                    public boolean isSelected(Join join) {
                        for(JoinChange tnj : joinsToFix) {
                            if(tnj.join == join) {
                                return false;
                            }
                        }
                        return true;
                    }

                    @Override
                    public boolean isSelected(Sequence sequence) {
                        return !excludedSequences.contains(sequence);
                    }
                }
        );
    }

    protected StorageFormatRegistry getStorageFormatRegistry() {
        return aisCloner.getStorageFormatRegistry();
    }

    /**
     * Returns the final, updated AkibanInformationSchema. This AIS has been fully 
     * validated and is frozen (no more changes), hence ready for update into the
     * server. 
     * @return - the primaryAIS, after merge() with the Table added.
     */
    public AkibanInformationSchema getAIS () {
        return targetAIS;
    }
    
    public AISMerge merge() {
        switch(mergeType) {
            case ADD_TABLE:
                doAddTableMerge();
            break;
            case MODIFY_TABLE:
                doModifyTableMerge();
            break;
            case ADD_INDEX:
                doAddIndexMerge();
            break;
            default:
                throw new IllegalStateException("Unknown MergeType: " + mergeType);
        }
        return this;
    }

    public Index mergeIndex(Index index) {
        if(index.isPrimaryKey()) {
            throw new ProtectedIndexException("PRIMARY", index.getIndexName().getFullTableName());
        }

        final IndexName indexName = index.getIndexName();
        final Index curIndex;
        final Index newIndex;
        final Group newGroup;
        switch(index.getIndexType()) {
            case TABLE:
            {
                final TableName tableName = new TableName(indexName.getSchemaName(), indexName.getTableName());
                final Table newTable = targetAIS.getTable(tableName);
                if(newTable == null) {
                    throw new NoSuchTableException(tableName);
                }
                curIndex = newTable.getIndex(indexName.getName());
                newGroup = newTable.getGroup();
                Integer newId = newIndexID(newGroup);
                newIndex = TableIndex.create(targetAIS, newTable, indexName.getName(), newId, index.isUnique(),
                                             index.getConstraint(), index.getConstraintName());
            }
            break;
            case GROUP:
            {
                GroupIndex gi = (GroupIndex)index;
                newGroup = targetAIS.getGroup(gi.getGroup().getName());
                if(newGroup == null) {
                    throw new NoSuchGroupException(gi.getGroup().getName());
                }
                curIndex = newGroup.getIndex(indexName.getName());
                Integer newId = newIndexID(newGroup);
                newIndex = GroupIndex.create(targetAIS, newGroup, indexName.getName(), newId, index.isUnique(),
                                             index.getConstraint(), index.getJoinType());
            }
            break;
            case FULL_TEXT:
            {
                final TableName tableName = new TableName(indexName.getSchemaName(), indexName.getTableName());
                final Table newTable = targetAIS.getTable(tableName);
                if(newTable == null) {
                    throw new NoSuchTableException(tableName);
                }
                curIndex = newTable.getFullTextIndex(indexName.getName());
                newGroup = newTable.getGroup();
                Integer newId = newIndexID(newGroup);
                newIndex = FullTextIndex.create(targetAIS, newTable, indexName.getName(), newId);
            }
            break;
            default:
                throw new IllegalArgumentException("Unknown index type: " + index);
        }

        if(index.getIndexMethod() == Index.IndexMethod.Z_ORDER_LAT_LON) {
            newIndex.markSpatial(index.firstSpatialArgument(), index.dimensions());
        }

        if(curIndex != null) {
            throw new DuplicateIndexException(indexName);
        }
        if(index.getKeyColumns().isEmpty()) {
            throw new IndexLacksColumnsException(indexName);
        }

        for(IndexColumn indexCol : index.getKeyColumns()) {
            final TableName refTableName = indexCol.getColumn().getTable().getName();
            final Table newRefTable = targetAIS.getTable(refTableName);
            if(newRefTable == null) {
                throw new NoSuchTableException(refTableName);
            }
            if(!newRefTable.getGroup().equals(newGroup)) {
                throw new TableNotInGroupException(refTableName);
            }

            final Column column = indexCol.getColumn();
            final Column newColumn = newRefTable.getColumn(column.getName());
            if(newColumn == null) {
                throw new NoSuchColumnException(column.getName());
            }
            // API call problem, not something user can generate
            if(!column.getType().typeClass().equals(newColumn.getType().typeClass())) {
                throw new IllegalArgumentException(
                    "Column type mismatch for " + column.getName() + ": " + column.getTypeName() + " vs " + newColumn.getTypeName()
                );
            }
            // Calls (Group)Index.addColumn(), which checks all are in same branch
            IndexColumn.create(newIndex, newColumn, indexCol, indexCol.getPosition());
        }

        newIndex.copyStorageDescription(index);
        getStorageFormatRegistry().finishStorageDescription(newIndex, nameGenerator);
        newIndex.freezeColumns();

        return newIndex;
    }

    private void doAddTableMerge() {
        // I should use TableSubsetWriter(new AISTarget(targetAIS))
        // but that assumes the Table.getAIS() is complete and valid. 
        // i.e. has a group and group table, joins are accurate, etc. 
        // this may not be true 
        // Also the tableIDs need to be assigned correctly, which 
        // TableSubsetWriter doesn't do. 
        LOG.debug("Merging new table {} into targetAIS", sourceTable.getName());

        final AISBuilder builder = new AISBuilder(targetAIS, nameGenerator,
                                                  getStorageFormatRegistry());

        Group targetGroup = null;
        if (sourceTable.getParentJoin() != null) {
            String parentSchemaName = sourceTable.getParentJoin().getParent().getName().getSchemaName();
            String parentTableName = sourceTable.getParentJoin().getParent().getName().getTableName(); 
            Table parentTable = targetAIS.getTable(parentSchemaName, parentTableName);
            if (parentTable == null) {
                throw new JoinToUnknownTableException (sourceTable.getName(), new TableName(parentSchemaName, parentTableName));
            }
            targetGroup = parentTable.getGroup();
        }

        // Add the user table to the targetAIS
        addTable (builder, sourceTable, targetGroup);

        // Joins or group table?
        if (sourceTable.getParentJoin() == null) {
            LOG.debug("Table is root or lone table");
            StorageDescription storage = null;
            if (sourceTable.getGroup() != null) {
                storage = sourceTable.getGroup().getStorageDescription();
            }
            addNewGroup(builder, sourceTable, storage);
        } else {
            // Normally there should be only one candidate parent join.
            // But since the AIS supports multiples, so does the merge.
            // This gets flagged in JoinToOneParent validation.
            for (Join join : sourceTable.getCandidateParentJoins()) {
                addJoin(builder, join, sourceTable);
            }
        }

        if (sourceTable.getPrimaryKeyIncludingInternal() != null) {
            TableIndex index = sourceTable.getPrimaryKeyIncludingInternal().getIndex();
            final int rootTableID = (targetGroup != null) ? 
                    targetGroup.getRoot().getTableId() : 
                        builder.akibanInformationSchema().getTable(sourceTable.getName()).getTableId();
            IndexName indexName = index.getIndexName();
            builder.index(sourceTable.getName().getSchemaName(), 
                    sourceTable.getName().getTableName(),
                    indexName.getName(), 
                    index.isUnique(), 
                    index.getConstraint(),
                    index.getConstraintName(),
                    nameGenerator.generateIndexID(rootTableID));
            for (IndexColumn col : index.getKeyColumns()) {
                    builder.indexColumn(sourceTable.getName().getSchemaName(), 
                            sourceTable.getName().getTableName(),
                            index.getIndexName().getName(),
                        col.getColumn().getName(), 
                        col.getPosition(), 
                        col.isAscending(), 
                        col.getIndexedLength());
            }
        }

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        
        for (TableIndex index : sourceTable.getIndexes()) {
            if (!index.isPrimaryKey() && !index.isForeignKey()) {
                mergeIndex(index);
            }
        }
        
        for (FullTextIndex index : sourceTable.getFullTextIndexes()) {
            mergeIndex(index);
        }
        
        for (ForeignKey fk : sourceTable.getReferencingForeignKeys()) {
            List<String> referencingColumnNames = new ArrayList<>();
            for (Column column : fk.getReferencingColumns()) {
                referencingColumnNames.add(column.getName());
            }
            List<String> referencedColumnNames = new ArrayList<>();
            for (Column column : fk.getReferencedColumns()) {
                referencedColumnNames.add(column.getName());
            }
            builder.foreignKey(sourceTable.getName().getSchemaName(), sourceTable.getName().getTableName(), referencingColumnNames,
                               fk.getReferencedTable().getName().getSchemaName(), fk.getReferencedTable().getName().getTableName(), referencedColumnNames,
                               fk.getDeleteAction(), fk.getUpdateAction(),
                               fk.isDeferrable(), fk.isInitiallyDeferred(),
                               fk.getConstraintName().getTableName());
        }

        builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        builder.akibanInformationSchema().freeze();
    }

    private void doModifyTableMerge() {
        AISBuilder builder = new AISBuilder(targetAIS, nameGenerator,
                                            getStorageFormatRegistry());

        // Fix up groups
        for(JoinChange tnj : changedJoins) {
            final Table table = targetAIS.getTable(tnj.newChildName);
            if(tnj.isNewGroup) {
                addNewGroup(builder, table, null);
            } else if(tnj.newParentName != null) {
                addJoin(builder, tnj.newParentName, tnj.parentCols, tnj.join, tnj.childCols, table);
            }
        }

        for(TableName name : groupsToClear) {
            Group group = targetAIS.getGroup(name);
            if(group != null) {
                group.setStorageDescription(null);
            }
        }

        // Ugly: groupingIsComplete() will set PRIMARY index tree names if missing.
        //       Clear them here as to only set them once.
        for(Map.Entry<IndexName,IndexInfo> entry : indexesToFix.entrySet()) {
            IndexName name = entry.getKey();
            IndexInfo info = entry.getValue();
            Table table = targetAIS.getTable(name.getSchemaName(), name.getTableName());
            Index index = table.getIndexIncludingInternal(name.getName());
            if(info.storage == null) {
                index.setStorageDescription(null);
            } else {
                index.setStorageDescription(info.storage.cloneForObject(index));
            }
        }

        for(IdentityInfo info : identityToFix) {
            addIdentitySequence(builder, info.tableName.getSchemaName(), info.tableName.getTableName(), info.columnName,
                                info.defaultIdentity, info.sequence);
        }

        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        for(Map.Entry<IndexName,IndexInfo> entry : indexesToFix.entrySet()) {
            IndexName name = entry.getKey();
            IndexInfo info = entry.getValue();
            Table table = targetAIS.getTable(name.getSchemaName(), name.getTableName());
            Index index = table.getIndexIncludingInternal(name.getName());
            index.setIndexId((info.id != null) ? info.id : newIndexID(table.getGroup()));
            if(info.storage == null && !index.isPrimaryKey()) {
                getStorageFormatRegistry().finishStorageDescription(index, nameGenerator);
            }
        }


        builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        builder.akibanInformationSchema().freeze();
    }

    private void doAddIndexMerge() {
        AISBuilder builder = new AISBuilder(targetAIS, nameGenerator,
                                            getStorageFormatRegistry());
        builder.groupingIsComplete();
        builder.akibanInformationSchema().validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        builder.akibanInformationSchema().freeze();
    }

    private void addTable(AISBuilder builder, final Table table, final Group targetGroup) {
        
        // I should use TableSubsetWriter(new AISTarget(targetAIS)) or AISCloner.clone()
        // but both assume the Table.getAIS() is complete and valid. 
        // i.e. has a group and group table, and the joins point to a valid table
        // which, given the use of AISMerge, is not true. 
        
        
        final String schemaName = table.getName().getSchemaName();
        final String tableName = table.getName().getTableName();
        

        builder.table(schemaName, tableName);
        Table targetTable = targetAIS.getTable(schemaName, tableName); 
        targetTable.setCharsetAndCollation(table.getCharsetName(), table.getCollationName());
        targetTable.setPendingOSC(table.getPendingOSC());
        targetTable.setUuid(table.getUuid());
        
        // columns
        for (Column column : table.getColumnsIncludingInternal()) {
            builder.column(schemaName, tableName, 
                    column.getName(), column.getPosition(), 
                    column.getType(), 
                    column.getInitialAutoIncrementValue() != null, 
                    column.getDefaultValue(), column.getDefaultFunction());
            Column newColumn = targetTable.getColumnsIncludingInternal().get(column.getPosition());
            newColumn.setUuid(column.getUuid());
            // if an auto-increment column, set the starting value. 
            if (column.getInitialAutoIncrementValue() != null) {
                newColumn.setInitialAutoIncrementValue(column.getInitialAutoIncrementValue());
            }
            if (column.getDefaultIdentity() != null) {
                addIdentitySequence(builder, schemaName, tableName, column.getName(),
                                    column.getDefaultIdentity(), column.getIdentityGenerator());
            }
            // Proactively cache, can go away if Column ever cleans itself up
            newColumn.getMaxStorageSize();
            newColumn.getPrefixSize();
        }
    }

    private void addIdentitySequence(AISBuilder builder, String schemaName, String tableName, String column,
                                     boolean defaultIdentity, Sequence sequence) {
        TableName sequenceName = nameGenerator.generateIdentitySequenceName(builder.akibanInformationSchema(),
                                                                            new TableName(schemaName, tableName),
                                                                            column);
        Sequence newSeq = builder.sequence(sequenceName.getSchemaName(), sequenceName.getTableName(),
                                           sequence.getStartsWith(),
                                           sequence.getIncrement(),
                                           sequence.getMinValue(),
                                           sequence.getMaxValue(),
                                           sequence.isCycle());
        builder.columnAsIdentity(schemaName, tableName, column, sequenceName.getTableName(), defaultIdentity);
        LOG.debug("Generated sequence: {}, with storage; {}", sequenceName, newSeq.getStorageNameString());
    }

    private void addNewGroup (AISBuilder builder, Table rootTable, StorageDescription copyStorage) {
        TableName groupName = rootTable.getName();
        builder.createGroup(groupName.getTableName(), groupName.getSchemaName(), 
                            copyStorage);
        builder.addTableToGroup(groupName,
                                rootTable.getName().getSchemaName(),
                                rootTable.getName().getTableName());
    }

    private void addJoin (AISBuilder builder, Join join, Table childTable) {
        Map<String,String> emptyMap = Collections.emptyMap();
        addJoin(builder, join.getParent().getName(), emptyMap, join, emptyMap, childTable);
    }

    private static String getOrDefault(Map<String, String> map, String key) {
        String val = map.get(key);
        return (val != null) ? val : key;
    }

    private void addJoin (AISBuilder builder, TableName parentName, Map<String,String> parentCols,
                          Join join, Map<String,String> childCols, Table childTable) {
        String parentSchemaName = parentName.getSchemaName();
        String parentTableName = parentName.getTableName();
        Table parentTable = targetAIS.getTable(parentSchemaName, parentTableName);
        if (parentTable == null) {
            throw new JoinToUnknownTableException(childTable.getName(), new TableName(parentSchemaName, parentTableName));
         }
        LOG.debug(String.format("Table is child of table %s", parentTable.getName().toString()));

        String joinName = join.getConstraintName().getTableName();
        builder.joinTables(joinName,
                parentSchemaName,
                parentTableName,
                childTable.getName().getSchemaName(),
                childTable.getName().getTableName()
                );

        for (JoinColumn joinColumn : join.getJoinColumns()) {
            try {
            builder.joinColumns(joinName,
                    parentSchemaName,
                    parentTableName,
                    getOrDefault(parentCols, joinColumn.getParent().getName()),
                    childTable.getName().getSchemaName(),
                    childTable.getName().getTableName(),
                    getOrDefault(childCols, joinColumn.getChild().getName()));
            } catch (AISBuilder.NoSuchObjectException ex) {
                throw new JoinToWrongColumnsException (
                        childTable.getName(), joinColumn.getChild().getName(),
                        new TableName(parentSchemaName, parentTableName),
                        joinColumn.getParent().getName());
            }
        }
        builder.basicSchemaIsComplete();
        
        try {
            builder.addJoinToGroup(parentTable.getGroup().getName(), joinName, 0);
        } catch (AISBuilder.GroupStructureException ex) {
            throw new JoinToMultipleParentsException(join.getChild().getName());
        }
    }

    private int newIndexID(Group group) {
        return newIndexID(group.getRoot().getTableId());
    }

    private int newIndexID(int rootTableID) {
        return nameGenerator.generateIndexID(rootTableID);
    }

    public static AkibanInformationSchema mergeView(AISCloner aisCloner, 
                                                    AkibanInformationSchema oldAIS,
                                                    View view) {
        AkibanInformationSchema newAIS = copyAISForAdd(aisCloner, oldAIS);
        copyView(newAIS, view);
        newAIS.validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        newAIS.freeze();
        return newAIS;
    }

    public static void copyView(AkibanInformationSchema newAIS,
                                View oldView) {
        Map<TableName,Collection<String>> newReferences = 
            new HashMap<>();
        for (Map.Entry<TableName,Collection<String>> entry : oldView.getTableColumnReferences().entrySet()) {
            newReferences.put(entry.getKey(),
                              new HashSet<>(entry.getValue()));
        }
        View newView = View.create(newAIS,
                                   oldView.getName().getSchemaName(),
                                   oldView.getName().getTableName(),
                                   oldView.getDefinition(),
                                   oldView.getDefinitionProperties(),
                                   newReferences);
        for (Column col : oldView.getColumns()) {
            Column.create(newView, col.getName(), col.getPosition(),
                          col.getType(), col.getInitialAutoIncrementValue());
        }
        newAIS.addView(newView);
    }
    
    public AkibanInformationSchema mergeSequence(Sequence sequence)
    {
        mergeSequenceInternal(sequence);
        targetAIS.validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        targetAIS.freeze();
        return targetAIS;
    }

    private Sequence mergeSequenceInternal(Sequence sequence)
    {
        Sequence newSeq = Sequence.create(targetAIS, sequence);
        newSeq.copyStorageDescription(sequence);
        getStorageFormatRegistry().finishStorageDescription(newSeq, nameGenerator);
        return newSeq;
    }

    public static AkibanInformationSchema mergeRoutine(AISCloner aisCloner,
                                                       AkibanInformationSchema oldAIS,
                                                       Routine routine) {
        AkibanInformationSchema newAIS = copyAISForAdd(aisCloner, oldAIS);
        newAIS.addRoutine(routine);
        newAIS.validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        newAIS.freeze();
        return newAIS;
    }

    public static AkibanInformationSchema mergeSQLJJar(AISCloner aisCloner,
                                                       AkibanInformationSchema oldAIS,
                                                       SQLJJar sqljJar) {
        AkibanInformationSchema newAIS = copyAISForAdd(aisCloner, oldAIS);
        newAIS.addSQLJJar(sqljJar);
        newAIS.validate(AISValidations.BASIC_VALIDATIONS).throwIfNecessary();
        newAIS.freeze();
        return newAIS;
    }
}
