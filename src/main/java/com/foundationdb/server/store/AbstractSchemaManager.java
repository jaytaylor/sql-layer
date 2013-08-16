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

package com.foundationdb.server.store;

import com.foundationdb.ais.AISCloner;
import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AISMerge;
import com.foundationdb.ais.model.AISTableNameChanger;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.NopVisitor;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Schema;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.ais.util.DDLGenerator;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.error.DuplicateRoutineNameException;
import com.foundationdb.server.error.DuplicateSQLJJarNameException;
import com.foundationdb.server.error.DuplicateSequenceNameException;
import com.foundationdb.server.error.DuplicateTableNameException;
import com.foundationdb.server.error.DuplicateViewException;
import com.foundationdb.server.error.ISTableVersionMismatchException;
import com.foundationdb.server.error.JoinToProtectedTableException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.NoSuchSQLJJarException;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.ProtectedTableDDLException;
import com.foundationdb.server.error.ReferencedSQLJJarException;
import com.foundationdb.server.error.ReferencedTableException;
import com.foundationdb.server.error.UndefinedViewException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.util.ReadWriteMap;
import com.foundationdb.util.ArgumentValidation;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

public abstract class AbstractSchemaManager implements Service, SchemaManager {
    public static enum SerializationType {
        NONE,
        PROTOBUF,
        UNKNOWN
    }

    public static final String MAX_AIS_SIZE_PROPERTY = "fdbsql.max_ais_size_bytes";
    public static final String SKIP_AIS_UPGRADE_PROPERTY = "fdbsql.skip_ais_upgrade";
    public static final SerializationType DEFAULT_SERIALIZATION = SerializationType.PROTOBUF;

    public static final String DEFAULT_CHARSET = "fdbsql.default_charset";
    public static final String DEFAULT_COLLATION = "fdbsql.default_collation";

    private static final String CREATE_SCHEMA_FORMATTER = "create schema if not exists `%s`;";
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSchemaManager.class.getName());

    protected final SessionService sessionService;
    protected final ConfigurationService config;
    protected final TransactionService txnService;

    protected SecurityService securityService;
    protected int maxAISBufferSize;
    protected SerializationType serializationType = SerializationType.NONE;
    protected ReadWriteMap<Integer,Integer> tableVersionMap;
    protected Map<TableName,MemoryTableFactory> memoryTableFactories;

    protected AbstractSchemaManager(ConfigurationService config, SessionService sessionService,
            TransactionService txnService) {
        this.config = config;
        this.sessionService = sessionService;
        this.txnService = txnService;
    }


    //
    // AbstractSchemaManager
    //

    protected interface ThrowingCallable<V> {
        public V runAndReturn(Session session) throws Exception;
    }

    protected static abstract class ThrowingRunnable implements ThrowingCallable<Void> {
        public abstract void run(Session session) throws Exception;

        public Void runAndReturn(Session session) throws Exception {
            run(session);
            return null;
        }
    }

    protected abstract NameGenerator getNameGenerator();
    /** validateAndFreeze, checkAndSerialize, buildRowDefCache */
    protected abstract void saveAISChangeWithRowDefs(Session session, AkibanInformationSchema newAIS, Collection<String> schemaNames);
    /** saveMemoryTables */
    protected abstract void serializeMemoryTables(Session session, AkibanInformationSchema newAIS);
    /** validateAndFreeze, serializeMemoryTables, buildRowDefCache */
    protected abstract void unSavedAISChangeWithRowDefs(Session session, AkibanInformationSchema newAIS);
    /** Run the given callable under a transaction, retrying if necessary. Session should be closed when finished. */
    protected abstract <V> V transactionally(Session session, ThrowingCallable<V> callable);
    /** Remove any persisted table status state associated with the given table. */
    protected abstract void clearTableStatus(Session session, UserTable table);


    //
    // Service
    //

    @Override
    public void start() {
        maxAISBufferSize = Integer.parseInt(config.getProperty(MAX_AIS_SIZE_PROPERTY));
        if(maxAISBufferSize < 0) {
            LOG.warn("Clamping property {} to 0", MAX_AIS_SIZE_PROPERTY);
            maxAISBufferSize = 0;
        }
        AkibanInformationSchema.setDefaultCharsetAndCollation(config.getProperty(DEFAULT_CHARSET),
                                                              config.getProperty(DEFAULT_COLLATION));
        this.memoryTableFactories = new HashMap<>();
        this.tableVersionMap = ReadWriteMap.wrapNonFair(new HashMap<Integer,Integer>());
    }

    @Override
    public void stop() {
        this.maxAISBufferSize = 0;
        this.serializationType = SerializationType.NONE;
        this.memoryTableFactories = null;
    }


    //
    // SchemaManager
    //

    @Override
    public TableName registerStoredInformationSchemaTable(final UserTable newTable, final int version) {
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                final TableName newName = newTable.getName();
                checkSystemSchema(newName, true);
                UserTable curTable = getAis(session).getUserTable(newName);
                if(curTable != null) {
                    Integer oldVersion = curTable.getVersion();
                    if(oldVersion != null && oldVersion == version) {
                        return;
                    } else {
                        throw new ISTableVersionMismatchException(oldVersion, version);
                    }
                }

                createTableCommon(session, newTable, true, version, null);
            }
        });
        return newTable.getName();
    }

    @Override
    public TableName registerMemoryInformationSchemaTable(final UserTable newTable, final MemoryTableFactory factory) {
        if(factory == null) {
            throw new IllegalArgumentException("MemoryTableFactory may not be null");
        }
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                createTableCommon(session, newTable, true, null, factory);
                memoryTableFactories.put(newTable.getName(), factory); // TODO: Fragile?
            }
        });
        return newTable.getName();
    }

    @Override
    public void unRegisterMemoryInformationSchemaTable(final TableName tableName) {
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                dropTableCommon(session, tableName, DropBehavior.RESTRICT, true, true);
            }
        });
        memoryTableFactories.remove(tableName);
    }

    @Override
    public TableName createTableDefinition(Session session, UserTable newTable) {
        return createTableCommon(session, newTable, false, null, null);
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        checkTableName(session, currentName, true, false);
        checkTableName(session, newName, false, false);

        final AkibanInformationSchema newAIS = AISCloner.clone(getAis(session));
        final UserTable newTable = newAIS.getUserTable(currentName);
        // Rename does not affect scan or modify data, bumping version not required

        AISTableNameChanger nameChanger = new AISTableNameChanger(newTable);
        nameChanger.setSchemaName(newName.getSchemaName());
        nameChanger.setNewTableName(newName.getTableName());
        nameChanger.doChange();

        // AISTableNameChanger doesn't bother with group names or group tables, fix them with the builder
        AISBuilder builder = new AISBuilder(newAIS);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        final String curSchema = currentName.getSchemaName();
        final String newSchema = newName.getSchemaName();
        if(curSchema.equals(newSchema)) {
            saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(curSchema));
        } else {
            saveAISChangeWithRowDefs(session, newAIS, Arrays.asList(curSchema, newSchema));
        }
    }

    @Override
    public Collection<Index> createIndexes(Session session, Collection<? extends Index> indexesToAdd, boolean keepTree) {
        
        // Per the interface specification, if the index list is empty do nothing
        // Avoid doing an empty merge too. 
        if (indexesToAdd.isEmpty()) { 
            return Collections.emptyList();
        }
        AISMerge merge = AISMerge.newForAddIndex(getNameGenerator(), getAis(session));
        Set<String> schemas = new HashSet<>();

        Collection<Integer> tableIDs = new ArrayList<>(indexesToAdd.size());
        Collection<Index> newIndexes = new ArrayList<>(indexesToAdd.size());
        for(Index proposed : indexesToAdd) {
            Index newIndex = merge.mergeIndex(proposed);
            if(keepTree) {
                newIndex.setTreeName(proposed.getTreeName());
            }
            newIndexes.add(newIndex);
            tableIDs.addAll(newIndex.getAllTableIDs());
            schemas.add(DefaultNameGenerator.schemaNameForIndex(newIndex));
        }
        merge.merge();
        AkibanInformationSchema newAIS = merge.getAIS();
        trackBumpTableVersion(session, newAIS, tableIDs);

        saveAISChangeWithRowDefs(session, newAIS, schemas);
        return newIndexes;
    }

    @Override
    public void dropIndexes(Session session, final Collection<? extends Index> indexesToDrop) {
        final AkibanInformationSchema newAIS = AISCloner.clone(
                getAis(session),
                new ProtobufWriter.TableSelector() {
                    @Override
                    public boolean isSelected(Columnar columnar) {
                        return true;
                    }

                    @Override
                    public boolean isSelected(Index index) {
                        return !indexesToDrop.contains(index);
                    }
        });

        Collection<Integer> tableIDs = new ArrayList<>(indexesToDrop.size());
        for(Index index : indexesToDrop) {
            tableIDs.addAll(index.getAllTableIDs());
        }
        trackBumpTableVersion(session, newAIS, tableIDs);

        final Set<String> schemas = new HashSet<>();
        for(Index index : indexesToDrop) {
            schemas.add(DefaultNameGenerator.schemaNameForIndex(index));
        }

        saveAISChangeWithRowDefs(session, newAIS, schemas);
    }

    @Override
    public void dropTableDefinition(Session session, String schemaName, String tableName, DropBehavior dropBehavior) {
        dropTableCommon(session, new TableName(schemaName, tableName), dropBehavior, false, false);
    }

    @Override
    public void alterTableDefinitions(Session session, Collection<ChangedTableDescription> alteredTables) {
        ArgumentValidation.isTrue("Altered list is not empty", !alteredTables.isEmpty());

        AkibanInformationSchema oldAIS = getAis(session);
        Set<String> schemas = new HashSet<>();
        List<Integer> tableIDs = new ArrayList<>(alteredTables.size());
        for(ChangedTableDescription desc : alteredTables) {
            TableName oldName = desc.getOldName();
            TableName newName = desc.getNewName();
            checkTableName(session, oldName, true, false);
            if(!oldName.equals(newName)) {
                checkTableName(session, newName, false, false);
            }
            UserTable newTable = desc.getNewDefinition();
            if(newTable != null) {
                checkJoinTo(newTable.getParentJoin(), newName, false);
            }
            schemas.add(oldName.getSchemaName());
            schemas.add(newName.getSchemaName());
            tableIDs.add(oldAIS.getUserTable(oldName).getTableId());
        }

        AISMerge merge = AISMerge.newForModifyTable(getNameGenerator(), getAis(session), alteredTables);
        merge.merge();
        AkibanInformationSchema newAIS = merge.getAIS();
        trackBumpTableVersion(session,newAIS, tableIDs);

        // This is hacky. PK trees have to be preserved because there is no way to duplicate
        // accumulator state that shouldn't change. But ordinals are stored in accumulators
        // and a new group dictates a new ordinal. Resetting to 0 causes a new one to be assigned.
        // Although ugly, it is safe because accumulators are transactional.
        for(ChangedTableDescription desc : alteredTables) {
            if(desc.isNewGroup()) {
                UserTable newTable = newAIS.getUserTable(desc.getOldName());
                newTable.setOrdinal(null);
            }
        }
        // Two passes to ensure all tables in a group are reset before beginning assignment
        for(ChangedTableDescription desc : alteredTables) {
            if(desc.isNewGroup()) {
                UserTable newTable = newAIS.getUserTable(desc.getOldName());
                assignNewOrdinal(newTable);
            }
        }

        saveAISChangeWithRowDefs(session, newAIS, schemas);
    }

    @Override
    public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition) {
        AkibanInformationSchema oldAIS = getAis(session);
        Sequence oldSequence = oldAIS.getSequence(sequenceName);
        if(oldSequence == null) {
            throw new NoSuchSequenceException(sequenceName);
        }

        if(!sequenceName.equals(newDefinition.getSequenceName())) {
            throw new UnsupportedOperationException("Renaming Sequence");
        }

        AkibanInformationSchema newAIS = AISCloner.clone(oldAIS);
        newAIS.removeSequence(sequenceName);
        Sequence newSequence = Sequence.create(newAIS, newDefinition);
        newSequence.setTreeName(getNameGenerator().generateSequenceTreeName(newDefinition));

        // newAIS may have mixed references to sequenceName. Re-clone to resolve.
        newAIS = AISCloner.clone(newAIS);

        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(sequenceName.getSchemaName()));
    }

    @Override
    public void createView(Session session, View view) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(view.getName(), false);
        if (oldAIS.getView(view.getName()) != null)
            throw new DuplicateViewException(view.getName());
        AkibanInformationSchema newAIS = AISMerge.mergeView(oldAIS, view);
        final String schemaName = view.getName().getSchemaName();
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
    }

    @Override
    public void dropView(Session session, TableName viewName) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(viewName, false);
        if (oldAIS.getView(viewName) == null)
            throw new UndefinedViewException(viewName);
        final AkibanInformationSchema newAIS = AISCloner.clone(oldAIS);
        newAIS.removeView(viewName);
        final String schemaName = viewName.getSchemaName();
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
    }

    /** Add the Sequence to the current AIS */
    @Override
    public void createSequence(Session session, Sequence sequence) {
        checkSequenceName(session, sequence.getSequenceName(), false);
        AISMerge merge = AISMerge.newForOther(getNameGenerator(), getAis(session));
        AkibanInformationSchema newAIS = merge.mergeSequence(sequence);
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(sequence.getSchemaName()));
    }

    /** Drop the given sequence from the current AIS. */
    @Override
    public void dropSequence(Session session, Sequence sequence) {
        checkSequenceName(session, sequence.getSequenceName(), true);
        List<TableName> emptyList = new ArrayList<>(0);
        final AkibanInformationSchema newAIS = removeTablesFromAIS(session, emptyList, Collections.singleton(sequence.getSequenceName()));
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(sequence.getSchemaName()));
    }

    @Override
    public void createRoutine(Session session, Routine routine, boolean replaceExisting) {
        createRoutineCommon(session, routine, false, replaceExisting);
    }

    @Override
    public void dropRoutine(Session session, TableName routineName) {
        dropRoutineCommon(session, routineName, false);
    }

    @Override
    public void createSQLJJar(Session session, SQLJJar sqljJar) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(sqljJar.getName(), false);
        if (oldAIS.getSQLJJar(sqljJar.getName()) != null)
            throw new DuplicateSQLJJarNameException(sqljJar.getName());
        final AkibanInformationSchema newAIS = AISMerge.mergeSQLJJar(oldAIS, sqljJar);
        final String schemaName = sqljJar.getName().getSchemaName();
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
    }

    @Override
    public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(sqljJar.getName(), false);
        SQLJJar oldJar = oldAIS.getSQLJJar(sqljJar.getName());
        if (oldJar == null)
            throw new NoSuchSQLJJarException(sqljJar.getName());
        final AkibanInformationSchema newAIS = AISCloner.clone(oldAIS);
        // Changing old state rather than actually replacing saves having to find
        // referencing routines, possibly in other schemas.
        oldJar = newAIS.getSQLJJar(sqljJar.getName());
        assert (oldJar != null);
        oldJar.setURL(sqljJar.getURL());
        final String schemaName = sqljJar.getName().getSchemaName();
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
    }

    @Override
    public void dropSQLJJar(Session session, TableName jarName) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(jarName, false);
        SQLJJar sqljJar = oldAIS.getSQLJJar(jarName);
        if (sqljJar == null)
            throw new NoSuchSQLJJarException(jarName);
        if (!sqljJar.getRoutines().isEmpty())
            throw new ReferencedSQLJJarException(sqljJar);
        final AkibanInformationSchema newAIS = AISCloner.clone(oldAIS);
        newAIS.removeSQLJJar(jarName);
        final String schemaName = jarName.getSchemaName();
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
    }

    @Override
    public void registerSystemRoutine(final Routine routine) {
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                createRoutineCommon(session, routine, true, false);
            }
        });
    }

    @Override
    public void unRegisterSystemRoutine(final TableName routineName) {
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                dropRoutineCommon(session, routineName, true);
            }
        });
    }

    @Override
    public Set<String> getTreeNames() {
        return getNameGenerator().getTreeNames();
    }

    @Override
    public boolean hasTableChanged(Session session, int tableID) {
        UserTable table = getAis(session).getUserTable(tableID);
        if(table == null) {
            throw new IllegalStateException("Unknown table: " + tableID);
        }
        // May be changed by ongoing DDL
        Map<Integer,Integer> changedVersions = session.get(TABLE_VERSIONS);
        Integer curVer = null;
        if(changedVersions != null) {
            curVer = changedVersions.get(tableID);
        }
        // Or not
        if(curVer == null) {
            curVer = tableVersionMap.get(tableID);
        }
        // Current may be null in pre-1.4.3 volumes
        Integer tableVer = table.getVersion();
        if(curVer == null) {
            return tableVer != null;
        }
        return !curVer.equals(tableVer);
    }

    @Override
    public void setSecurityService(SecurityService securityService) {
        this.securityService = securityService;
    }


    //
    // Internal methods
    //

    private TableName createTableCommon(Session session, UserTable newTable, boolean isInternal,
                                        Integer version, MemoryTableFactory factory) {
        final TableName newName = newTable.getName();
        checkTableName(session, newName, false, isInternal);
        checkJoinTo(newTable.getParentJoin(), newName, isInternal);

        if (newTable.getUuid() == null)
            newTable.setUuid(UUID.randomUUID());
        for (Column newColumn : newTable.getColumns()) {
            if (newColumn.getUuid() == null)
                newColumn.setUuid(UUID.randomUUID());
        }

        AISMerge merge = AISMerge.newForAddTable(getNameGenerator(), getAis(session), newTable);
        merge.merge();
        AkibanInformationSchema newAIS = merge.getAIS();
        UserTable mergedTable = newAIS.getUserTable(newName);

        if(version == null) {
            version = 0; // New user or memory table
        }
        mergedTable.setVersion(version);
        tableVersionMap.putNewKey(mergedTable.getTableId(), version);

        assignNewOrdinal(mergedTable);

        if(factory == null) {
            saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(newName.getSchemaName()));
        } else {
            // Memory only table changed, no reason to re-serialize
            mergedTable.setMemoryTableFactory(factory);
            unSavedAISChangeWithRowDefs(session, newAIS);
        }
        return newName;
    }

    private void dropTableCommon(Session session, TableName tableName, final DropBehavior dropBehavior,
                                 final boolean isInternal, final boolean mustBeMemory) {
        checkTableName(session, tableName, true, isInternal);
        final UserTable table = getAis(session).getUserTable(tableName);

        final List<TableName> tables = new ArrayList<>();
        final Set<String> schemas = new HashSet<>();
        final List<Integer> tableIDs = new ArrayList<>();
        final Set<TableName> sequences = new HashSet<>();

        // Collect all tables in branch below this point
        table.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable userTable) {
                if(mustBeMemory && !userTable.hasMemoryTableFactory()) {
                    throw new IllegalArgumentException("Cannot un-register non-memory table");
                }

                if((dropBehavior == DropBehavior.RESTRICT) && !userTable.getChildJoins().isEmpty()) {
                    throw new ReferencedTableException (table);
                }

                TableName name = userTable.getName();
                tables.add(name);
                schemas.add(name.getSchemaName());
                tableIDs.add(userTable.getTableId());
                for (Column column : userTable.getColumnsIncludingInternal()) {
                    if (column.getIdentityGenerator() != null) {
                        sequences.add(column.getIdentityGenerator().getSequenceName());
                    }
                }
            }
        });

        final AkibanInformationSchema oldAIS = getAis(session);
        final AkibanInformationSchema newAIS = removeTablesFromAIS(session, tables, sequences);
        trackBumpTableVersion(session, newAIS, tableIDs);

        for(Integer tableID : tableIDs) {
            clearTableStatus(session, oldAIS.getUserTable(tableID));
        }

        if(table.hasMemoryTableFactory()) {
            unSavedAISChangeWithRowDefs(session, newAIS);
        } else {
            saveAISChangeWithRowDefs(session, newAIS, schemas);
        }
    }

    private void createRoutineCommon(Session session, Routine routine,
                                     boolean inSystem, boolean replaceExisting) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(routine.getName(), inSystem);
        if (!replaceExisting && (oldAIS.getRoutine(routine.getName()) != null))
            throw new DuplicateRoutineNameException(routine.getName());
        final AkibanInformationSchema newAIS = AISMerge.mergeRoutine(oldAIS, routine);
        if (inSystem)
            unSavedAISChangeWithRowDefs(session, newAIS);
        else {
            final String schemaName = routine.getName().getSchemaName();
            saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
        }
    }

    private void dropRoutineCommon(Session session, TableName routineName, boolean inSystem) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(routineName, inSystem);
        Routine routine = oldAIS.getRoutine(routineName);
        if (routine == null)
            throw new NoSuchRoutineException(routineName);
        final AkibanInformationSchema newAIS = AISCloner.clone(oldAIS);
        routine = newAIS.getRoutine(routineName);
        newAIS.removeRoutine(routineName);
        if (routine.getSQLJJar() != null)
            routine.getSQLJJar().removeRoutine(routine); // Keep accurate in memory.
        if (inSystem)
            unSavedAISChangeWithRowDefs(session, newAIS);
        else {
            final String schemaName = routineName.getSchemaName();
            saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(schemaName));
        }
    }

    /**
     * Construct a new AIS instance containing a copy of the currently known data, see @{link #ais},
     * minus the given list of TableNames.
     * @param tableNames List of tables to exclude from new AIS.
     * @return A completely new AIS.
     */
    private AkibanInformationSchema removeTablesFromAIS(Session session, final List<TableName> tableNames, final Set<TableName> sequences) {
        return AISCloner.clone(
                getAis(session),
                new ProtobufWriter.TableSelector() {
                    @Override
                    public boolean isSelected(Columnar columnar) {
                        return !tableNames.contains(columnar.getName());
                    }

                    @Override
                    public boolean isSelected(Index index) {
                        if(index.isTableIndex()) {
                            return true;
                        }
                        for(IndexColumn icol : index.getKeyColumns()) {
                            if(tableNames.contains(icol.getColumn().getTable().getName())) {
                                return false;
                            }
                        }
                        return true;
                    }

                    @Override
                    public boolean isSelected(Sequence sequence) {
                        return !sequences.contains(sequence.getSequenceName());
                    }
                }
        );
    }

    protected void trackBumpTableVersion (Session session, AkibanInformationSchema newAIS, Collection<Integer> affectedIDs)
    {
        // Schedule the update for the tableVersionMap version number on commit.
        // A single DDL may trigger multiple tracks (e.g. ALTER affecting a GI), so only bump once per DDL.
        Map<Integer,Integer> tableAndVersions = session.get(TABLE_VERSIONS);
        if(tableAndVersions == null) {
            tableAndVersions = new HashMap<>();
            session.put(TABLE_VERSIONS, tableAndVersions);
            txnService.addCallback(session, TransactionService.CallbackType.COMMIT, bumpTableVersionCommit);
            txnService.addCallback(session, TransactionService.CallbackType.END, cleanTableVersion);
        }

        // Set the new table version  for tables in the NewAIS
        for(Integer tableID : affectedIDs) {
            Integer newVersion = tableAndVersions.get(tableID);
            if(newVersion == null) {
                Integer current = tableVersionMap.get(tableID);
                newVersion = (current == null) ? 1 : current + 1;
                tableAndVersions.put(tableID, newVersion);
            }
            UserTable table = newAIS.getUserTable(tableID);
            if(table != null) { // From a drop
                table.setVersion(newVersion);
            }
        }
    }

    protected final static Session.MapKey<Integer,Integer> TABLE_VERSIONS = Session.MapKey.mapNamed("TABLE_VERSIONS");

    // If the Alter table fails, make sure to clean up the TABLE_VERSION change list on end
    // If the Alter succeeds, the bumpTableVersionCommit process will clean up, and this does nothing. 
    protected final TransactionService.Callback cleanTableVersion = new TransactionService.Callback() {
        
        @Override
        public void run(Session session, long timestamp) {
            session.remove(TABLE_VERSIONS);
        }
    };
    
    
    protected final TransactionService.Callback bumpTableVersionCommit = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            Map<Integer,Integer> tableAndVersions = session.remove(TABLE_VERSIONS);
            if(tableAndVersions != null) {
                bumpTableVersions(tableAndVersions);
            }
        }
    };
    
    private void bumpTableVersions(Map<Integer,Integer> tableAndVersions) {
        for(Entry<Integer, Integer> entry : tableAndVersions.entrySet()) {
            int tableID = entry.getKey();
            int newVersion = entry.getValue();
            Integer current = tableVersionMap.get(entry.getKey());
            if(current != null && current >= newVersion) {
                throw new IllegalStateException("Current not < new: " + current + "," + newVersion);
            }
            boolean success = tableVersionMap.compareAndSet(tableID, current, newVersion);
            // Failed CAS would indicate concurrent DDL on this table, which should not be possible
            if(!success) {
                throw new IllegalStateException("Unexpected concurrent DDL on table: " + tableID);
            }
        }
    }

    private static void assignNewOrdinal(final UserTable newTable) {
        assert newTable.getOrdinal() == null : newTable + ": " + newTable.getOrdinal();
        MaxOrdinalVisitor visitor = new MaxOrdinalVisitor();
        newTable.getGroup().getRoot().traverseTableAndDescendants(visitor);
        newTable.setOrdinal(visitor.maxOrdinal + 1);
    }

    private static void checkSystemSchema(TableName tableName, boolean shouldBeSystem) {
        String schemaName = tableName.getSchemaName();
        final boolean inSystem = TableName.INFORMATION_SCHEMA.equals(schemaName) ||
                                 TableName.SECURITY_SCHEMA.equals(schemaName) ||
                                 TableName.SYS_SCHEMA.equals(schemaName) ||
                                 TableName.SQLJ_SCHEMA.equals(schemaName);
        if(shouldBeSystem && !inSystem) {
            throw new IllegalArgumentException("Table required to be in "+TableName.INFORMATION_SCHEMA +" schema");
        }
        if(!shouldBeSystem && inSystem) {
            throw new ProtectedTableDDLException(tableName);
        }
    }

    private static void checkJoinTo(Join join, TableName childName, boolean isInternal) {
        TableName parentName = (join != null) ? join.getParent().getName() : null;
        if(parentName != null) {
            String parentSchema = parentName.getSchemaName();
            boolean inAIS = (TableName.INFORMATION_SCHEMA.equals(parentSchema) ||
                             TableName.SECURITY_SCHEMA.equals(parentSchema));
            if(inAIS && !isInternal) {
                throw new JoinToProtectedTableException(parentName, childName);
            } else if(!inAIS && isInternal) {
                throw new IllegalArgumentException("Internal table join to non-IS table: " + childName);
            }
        }
    }

    private void checkTableName(Session session, TableName tableName, boolean shouldExist, boolean inIS) {
        checkSystemSchema(tableName, inIS);
        if (!inIS && (securityService != null) &&
            !securityService.isAccessible(session, tableName.getSchemaName())) {
            throw new ProtectedTableDDLException(tableName);
        }
        final boolean tableExists = getAis(session).getTable(tableName) != null;
        if(shouldExist && !tableExists) {
            throw new NoSuchTableException(tableName);
        }
        if(!shouldExist && tableExists) {
            throw new DuplicateTableNameException(tableName);
        }
    }

    private void checkSequenceName(Session session, TableName sequenceName, boolean shouldExist) {
        checkSystemSchema (sequenceName, false);
        final boolean exists = getAis(session).getSequence(sequenceName) != null;
        if (shouldExist && !exists) {
            throw new NoSuchSequenceException(sequenceName);
        }
        if (!shouldExist && exists) {
            throw new DuplicateSequenceNameException(sequenceName);
        }
    }

    private static class MaxOrdinalVisitor extends NopVisitor {
        public int maxOrdinal = 0;

        @Override
        public void visitUserTable(UserTable table) {
            Integer ordinal = table.getOrdinal();
            if((ordinal != null) && (ordinal > maxOrdinal)) {
                maxOrdinal = ordinal;
            }
        }
    }
}
