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
import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.View;
import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.ChangedTableDescription;
import com.foundationdb.qp.memoryadapter.MemoryTableFactory;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.DuplicateRoutineNameException;
import com.foundationdb.server.error.DuplicateSQLJJarNameException;
import com.foundationdb.server.error.DuplicateSequenceNameException;
import com.foundationdb.server.error.DuplicateTableNameException;
import com.foundationdb.server.error.DuplicateViewException;
import com.foundationdb.server.error.ISTableVersionMismatchException;
import com.foundationdb.server.error.NoColumnsInTableException;
import com.foundationdb.server.error.NoSuchRoutineException;
import com.foundationdb.server.error.NoSuchSQLJJarException;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.OnlineDDLInProgressException;
import com.foundationdb.server.error.ProtectedTableDDLException;
import com.foundationdb.server.error.ReferencedSQLJJarException;
import com.foundationdb.server.error.ReferencedTableException;
import com.foundationdb.server.error.UndefinedViewException;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.Session.Key;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.transaction.TransactionService.Callback;
import com.foundationdb.server.service.transaction.TransactionService.CallbackType;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.StorageFormatRegistry;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.server.types.service.TypesRegistry;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.util.ArgumentValidation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
import java.util.UUID;

public abstract class AbstractSchemaManager implements Service, SchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSchemaManager.class);

    public static final String SKIP_AIS_UPGRADE_PROPERTY = "fdbsql.skip_ais_upgrade";

    public static final String COLLATION_MODE = "fdbsql.collation_mode";
    public static final String DEFAULT_CHARSET = "fdbsql.default_charset";
    public static final String DEFAULT_COLLATION = "fdbsql.default_collation";

    private static final Key<OnlineSession> ONLINE_SESSION_KEY = Key.named("SM_ONLINE");
    private static final Object ONLINE_CACHE_KEY = new Object();

    protected final SessionService sessionService;
    protected final ConfigurationService config;
    protected final TransactionService txnService;
    protected final TypesRegistryService typesRegistryService;
    protected final StorageFormatRegistry storageFormatRegistry;
    protected TypesTranslator typesTranslator;
    protected AISCloner aisCloner;

    protected SecurityService securityService;

    protected AbstractSchemaManager(ConfigurationService config, SessionService sessionService,
                                    TransactionService txnService, TypesRegistryService typesRegistryService,
                                    StorageFormatRegistry storageFormatRegistry) {
        this.config = config;
        this.sessionService = sessionService;
        this.txnService = txnService;
        this.typesRegistryService = typesRegistryService;
        this.storageFormatRegistry = storageFormatRegistry;
    }


    //
    // Derived
    //

    protected abstract NameGenerator getNameGenerator(Session session);
    /** Get the primary (i.e. *not* online) AIS for {@code session}. Load from disk if necessary. */
    protected abstract  AkibanInformationSchema getSessionAIS(Session session);
    /** validateAndFreeze, checkAndSerialize, buildRowDefs */
    protected abstract void storedAISChange(Session session,
                                            AkibanInformationSchema newAIS,
                                            Collection<String> schemas);
    /** validateAndFreeze, serializeMemoryTables, buildRowDefs */
    protected abstract void unStoredAISChange(Session session, AkibanInformationSchema newAIS);
    /** Called immediately prior to {@link #storedAISChange} when renaming a table */
    protected abstract void renamingTable(Session session, TableName oldName, TableName newName);
    /** Remove any persisted table status state associated with the given table. */
    protected abstract void clearTableStatus(Session session, Table table);
    /** Mark a new generation */
    protected abstract void bumpGeneration(Session session);
    /** Generate and save a new ID. */
    protected abstract long generateSaveOnlineSessionID(Session session);
    /** validateAndFreeze, checkAndSerialize, buildRowDefs. */
    protected abstract void storedOnlineChange(Session session,
                                               OnlineSession onlineSession,
                                               AkibanInformationSchema newAIS,
                                               Collection<String> schemas);
    /** Remove any online state stored for the session. */
    protected abstract void clearOnlineState(Session session, OnlineSession onlineSession);
    /** Read and populate from storage. */
    protected abstract OnlineCache buildOnlineCache(Session session);
    /** Hook for new table versions (id -> version). Note: Not yet committed. **/
    protected abstract void newTableVersions(Session session, Map<Integer,Integer> versions);


    //
    // AbstractSchemaManager
    //

    protected OnlineSession getOnlineSession(Session session, Boolean shouldBePresent) {
        OnlineSession onlineSession = session.get(ONLINE_SESSION_KEY);
        if(shouldBePresent == null) {
            return onlineSession;
        }
        if(shouldBePresent && (onlineSession == null)) {
            throw new IllegalStateException("no online session: " + session);
        } else if(!shouldBePresent && (onlineSession != null)) {
            throw new IllegalStateException("online session already exists: " + session);
        }
        return onlineSession;
    }

    protected OnlineCache getOnlineCache(final Session session, AkibanInformationSchema ais) {
        // Every DDL bumps generation (online or not) so the progress state is valid to be cached
        OnlineCache cache = ais.getCachedValue(ONLINE_CACHE_KEY, null);
        if(cache == null) {
            cache = ais.getCachedValue(
                ONLINE_CACHE_KEY,
                new CacheValueGenerator<OnlineCache>() {
                    @Override
                    public OnlineCache valueFor(AkibanInformationSchema ais) {
                        return buildOnlineCache(session);
                    }
                }
            );
        }
        return cache;
    }

    protected void registerSystemTables() {
    }

    protected void bumpTableVersions(Session session, AkibanInformationSchema newAIS, Collection<Integer> affectedIDs) {
        if(affectedIDs.isEmpty()) {
            return;
        }
        AkibanInformationSchema curAIS = getAISForChange(session);
        Map<Integer,Integer> newVersions = new HashMap<>();
        // Set the new table version for tables in the NewAIS
        for(Integer tableID : affectedIDs) {
            Table curTable = curAIS.getTable(tableID);
            assert (curTable != null): "null table for bump: " + tableID;
            int newVersion = curTable.getVersion() + 1;
            Table newTable = newAIS.getTable(tableID);
            // From a drop
            if(newTable != null) {
                newTable.setVersion(newVersion);
            }
            newVersions.put(tableID, newVersion);
            LOG.trace("Table {} now at version {}", tableID, newVersion);
        }
        newTableVersions(session, newVersions);
    }


    //
    // Service
    //

    @Override
    public void start() {
        // TODO: AkCollatorFactory should probably be a service
        AkCollatorFactory.setCollationMode(config.getProperty(COLLATION_MODE));
        AkibanInformationSchema.setDefaultCharsetAndCollation(config.getProperty(DEFAULT_CHARSET),
                                                              config.getProperty(DEFAULT_COLLATION));
        this.typesTranslator = MTypesTranslator.INSTANCE; // TODO: Move to child.
        this.aisCloner = new AISCloner(typesRegistryService.getTypesRegistry(),
                                       storageFormatRegistry);
        storageFormatRegistry.registerStandardFormats();
    }

    @Override
    public void stop() {
    }


    //
    // SchemaManager
    //

    @Override
    public TableName registerStoredInformationSchemaTable(final Table newTable, final int version) {
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    final TableName newName = newTable.getName();
                    checkSystemSchema(newName, true);
                    Table curTable = getAISForChange(session, false).getTable(newName);
                    if(curTable != null) {
                        Integer oldVersion = curTable.getVersion();
                        if(oldVersion != null && oldVersion == version) {
                            return;
                        } else {
                            throw new ISTableVersionMismatchException(oldVersion, version);
                        }
                    }

                    createTableCommon(session, newTable, true, version, false);
                }
            });
        }
        return newTable.getName();
    }

    @Override
    public TableName registerMemoryInformationSchemaTable(final Table newTable, final MemoryTableFactory factory) {
        if(factory == null) {
            throw new IllegalArgumentException("MemoryTableFactory may not be null");
        }
        final Group group = newTable.getGroup();
        // Factory will actually get applied at the end of AISMerge.merge() onto
        // a new table.
        storageFormatRegistry.registerMemoryFactory(group.getName(), factory);
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    storageFormatRegistry.finishStorageDescription(group, getNameGenerator(session));
                    createTableCommon(session, newTable, true, null, true);
                }
            });
        }
        return newTable.getName();
    }

    @Override
    public void unRegisterMemoryInformationSchemaTable(final TableName tableName) {
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    dropTableCommon(session, tableName, DropBehavior.RESTRICT, true, true);
                }
            });
        }
        storageFormatRegistry.unregisterMemoryFactory(tableName);
    }

    @Override
    public boolean isOnlineActive(Session session, int tableID) {
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        Long onlineID = onlineCache.tableToOnline.get(tableID);
        boolean isActive = (onlineID != null);
        if(isActive) {
            OnlineSession onlineSession = getOnlineSession(session, null);
            isActive = (onlineSession == null) || (onlineSession.id != onlineID);
        }
        return isActive;
    }

    @Override
    public Collection<OnlineChangeState> getOnlineChangeStates(Session session) {
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        List<OnlineChangeState> states = new ArrayList<>();
        for(Entry<Long, AkibanInformationSchema> entry : onlineCache.onlineToAIS.entrySet()) {
            AkibanInformationSchema onlineAIS = entry.getValue();
            Collection<ChangeSet> changeSets = onlineCache.onlineToChangeSets.get(entry.getKey());
            states.add(new ReadOnlyOnlineChangeState(onlineAIS, changeSets));
        }
        return states;
    }

    @Override
    public void startOnline(Session session) {
        getOnlineSession(session, false);
        long id = generateSaveOnlineSessionID(session);
        LOG.debug("Generated OnlineSession id: {}", id);
        OnlineSession onlineSession = new OnlineSession(id);
        session.put(ONLINE_SESSION_KEY, onlineSession);
        txnService.addCallback(session, CallbackType.ROLLBACK, REMOVE_ONLINE_SESSION_KEY_CALLBACK);
    }

    @Override
    public AkibanInformationSchema getOnlineAIS(Session session) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        AkibanInformationSchema sessionAIS = getSessionAIS(session);
        OnlineCache cache = getOnlineCache(session, sessionAIS);
        AkibanInformationSchema onlineAIS = cache.onlineToAIS.get(onlineSession.id);
        return (onlineAIS != null) ? onlineAIS : sessionAIS;
    }

    @Override
    public Collection<ChangeSet> getOnlineChangeSets(Session session) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        OnlineCache onlineCache = getOnlineCache(session, getSessionAIS(session));
        Collection<ChangeSet> changeSets = onlineCache.onlineToChangeSets.get(onlineSession.id);
        if(changeSets == null) {
            changeSets = Collections.emptyList();
        }
        return changeSets;
    }

    @Override
    public void finishOnline(Session session) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        AkibanInformationSchema ais = getOnlineAIS(session);
        AkibanInformationSchema newAIS = aisCloner.clone(ais);
        bumpTableVersions(session, newAIS, onlineSession.tableIDs);
        storedAISChange(session, newAIS, onlineSession.schemaNames);
        clearOnlineState(session, onlineSession);
        txnService.addCallback(session, CallbackType.COMMIT, REMOVE_ONLINE_SESSION_KEY_CALLBACK);
    }

    @Override
    public void discardOnline(Session session) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        clearOnlineState(session, onlineSession);
        bumpGeneration(session);
        txnService.addCallback(session, CallbackType.COMMIT, REMOVE_ONLINE_SESSION_KEY_CALLBACK);
    }

    @Override
    public TableName createTableDefinition(Session session, Table newTable) {
        return createTableCommon(session, newTable, false, null, false);
    }

    @Override
    public void renameTable(Session session, TableName currentName, TableName newName) {
        checkTableName(session, currentName, true, false);
        checkTableName(session, newName, false, false);

        final AkibanInformationSchema newAIS = aisCloner.clone(getAISForChange(session));
        final Table newTable = newAIS.getTable(currentName);
        // Rename does not affect scan or modify data, bumping version not required

        AISTableNameChanger nameChanger = new AISTableNameChanger(newTable);
        nameChanger.setSchemaName(newName.getSchemaName());
        nameChanger.setNewTableName(newName.getTableName());
        nameChanger.doChange();

        // AISTableNameChanger doesn't bother with group names or group tables, fix them with the builder
        AISBuilder builder = new AISBuilder(newAIS);
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();

        renamingTable(session, currentName, newName);
        final String curSchema = currentName.getSchemaName();
        final String newSchema = newName.getSchemaName();
        final List<String> schemaNames;
        if(curSchema.equals(newSchema)) {
            schemaNames = Arrays.asList(curSchema);
        } else {
            schemaNames = Arrays.asList(curSchema, newSchema);
        }
        saveAISChange(session, newAIS, schemaNames);
    }

    @Override
    public void createIndexes(Session session, Collection<? extends Index> indexes, boolean keepStorage) {
        ArgumentValidation.notNull("indexes", indexes);
        ArgumentValidation.notEmpty("indexes", indexes);

        AISMerge merge = AISMerge.newForAddIndex(aisCloner, getNameGenerator(session), getAISForChange(session));
        Set<String> schemas = new HashSet<>();
        Collection<Integer> tableIDs = new HashSet<>(indexes.size());
        for(Index proposed : indexes) {
            Index newIndex = merge.mergeIndex(proposed);
            if(keepStorage && (proposed.getStorageDescription() != null)) {
                newIndex.copyStorageDescription(proposed);
            }
            tableIDs.addAll(newIndex.getAllTableIDs());
            schemas.add(DefaultNameGenerator.schemaNameForIndex(newIndex));
        }
        merge.merge();
        AkibanInformationSchema newAIS = merge.getAIS();
        saveAISChange(session, newAIS, schemas, tableIDs);
    }

    @Override
    public void dropIndexes(Session session, final Collection<? extends Index> indexesToDrop) {
        final AkibanInformationSchema newAIS = aisCloner.clone(
                getAISForChange(session),
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
        final Set<String> schemas = new HashSet<>();
        for(Index index : indexesToDrop) {
            schemas.add(DefaultNameGenerator.schemaNameForIndex(index));
        }
        saveAISChange(session, newAIS, schemas, tableIDs);
    }

    @Override
    public void dropTableDefinition(Session session, String schemaName, String tableName, DropBehavior dropBehavior) {
        dropTableCommon(session, new TableName(schemaName, tableName), dropBehavior, false, false);
    }


    @Override
    public void dropSchema(Session session, String schemaName) {

        AkibanInformationSchema newAIS = removeSchemaFromAIS(getAISForChange(session), schemaName);
        saveAISChange(session, newAIS, Collections.singleton(schemaName));
    }

    @Override
    public void dropNonSystemSchemas(Session session) {
        AkibanInformationSchema aisForChange = getAISForChange(session);
        List<String> affectedSchemas = new ArrayList<>();
        for (String schemaName : aisForChange.getSchemas().keySet()) {
            affectedSchemas.add(schemaName);
        }
        AkibanInformationSchema newAIS = aisCloner.clone(aisForChange,
                new ProtobufWriter.WriteSelector() {
                    @Override
                    public Columnar getSelected(Columnar columnar) {
                        return columnar.getName().inSystemSchema() ? columnar : null;
                    }

                    @Override
                    public boolean isSelected(Group group) {
                        return group.getName().inSystemSchema();
                    }

                    @Override
                    public boolean isSelected(Join parentJoin) {
                        return parentJoin.getConstraintName().inSystemSchema();
                    }

                    @Override
                    public boolean isSelected(Index index) {
                        return index.getIndexName().getFullTableName().inSystemSchema();
                    }

                    @Override
                    public boolean isSelected(Sequence sequence) {
                        return sequence.getSequenceName().inSystemSchema();
                    }

                    @Override
                    public boolean isSelected(Routine routine) {
                        return routine.getName().inSystemSchema();
                    }

                    @Override
                    public boolean isSelected(SQLJJar sqljJar) {
                        return sqljJar.getName().inSystemSchema();
                    }

                    @Override
                    public boolean isSelected(ForeignKey foreignKey) {
                        return foreignKey.getConstraintName().inSystemSchema();
                    }
                });
        saveAISChange(session, newAIS, affectedSchemas);
    }

    @Override
    public void alterTableDefinitions(Session session, Collection<ChangedTableDescription> alteredTables) {
        ArgumentValidation.notEmpty("alteredTables", alteredTables);

        AkibanInformationSchema oldAIS = getAISForChange(session);
        Set<String> schemas = new HashSet<>();
        List<Integer> tableIDs = new ArrayList<>(alteredTables.size());
        for(ChangedTableDescription desc : alteredTables) {
            TableName oldName = desc.getOldName();
            TableName newName = desc.getNewName();
            checkTableName(session, oldName, true, false);
            if(!oldName.equals(newName)) {
                checkTableName(session, newName, false, false);
            }
            Table newTable = desc.getNewDefinition();
            if(newTable != null) {
                AISInvariants.checkJoinTo(newTable.getParentJoin(), newName, false);
                if(newTable.getColumns().isEmpty()) {
                    throw new NoColumnsInTableException(newName);
                }
            }
            schemas.add(oldName.getSchemaName());
            schemas.add(newName.getSchemaName());
            tableIDs.add(oldAIS.getTable(oldName).getTableId());
        }

        AISMerge merge = AISMerge.newForModifyTable(aisCloner, getNameGenerator(session), oldAIS, alteredTables);
        merge.merge();
        final AkibanInformationSchema newAIS = merge.getAIS();

        for(ChangedTableDescription desc : alteredTables) {
            Table newTable = newAIS.getTable(desc.getNewName());
            ensureUuids(newTable);

            // New groups require new ordinals
            if(desc.isNewGroup()) {
                newTable.setOrdinal(null);
            }
        }

        // Two passes to ensure all tables in a group are reset before beginning assignment
        for(ChangedTableDescription desc : alteredTables) {
            if(desc.isNewGroup()) {
                Table newTable = newAIS.getTable(desc.getOldName());
                assignNewOrdinal(newTable);
            }
        }

        saveAISChange(session, newAIS, schemas, tableIDs);
    }

    @Override
    public void alterSequence(Session session, TableName sequenceName, Sequence newDefinition) {
        AkibanInformationSchema oldAIS = getAISForChange(session);
        Sequence oldSequence = oldAIS.getSequence(sequenceName);
        if(oldSequence == null) {
            throw new NoSuchSequenceException(sequenceName);
        }

        if(!sequenceName.equals(newDefinition.getSequenceName())) {
            throw new UnsupportedOperationException("Renaming Sequence");
        }

        AkibanInformationSchema newAIS = aisCloner.clone(oldAIS);
        newAIS.removeSequence(sequenceName);
        Sequence newSequence = Sequence.create(newAIS, newDefinition);
        storageFormatRegistry.finishStorageDescription(newSequence, getNameGenerator(session));

        // newAIS may have mixed references to sequenceName. Re-clone to resolve.
        newAIS = aisCloner.clone(newAIS);

        saveAISChange(session, newAIS, Collections.singleton(sequenceName.getSchemaName()));
    }

    @Override
    public final AkibanInformationSchema getAis(Session session) {
        return getSessionAIS(session);
    }

    @Override
    public void createView(Session session, View view) {
        final AkibanInformationSchema oldAIS = getAISForChange(session);
        checkSystemSchema(view.getName(), false);
        if (oldAIS.getView(view.getName()) != null)
            throw new DuplicateViewException(view.getName());
        AkibanInformationSchema newAIS = AISMerge.mergeView(aisCloner, oldAIS, view);
        final String schemaName = view.getName().getSchemaName();
        saveAISChange(session, newAIS, Collections.singleton(schemaName));
    }

    @Override
    public void dropView(Session session, TableName viewName) {
        final AkibanInformationSchema oldAIS = getAISForChange(session);
        checkSystemSchema(viewName, false);
        if (oldAIS.getView(viewName) == null)
            throw new UndefinedViewException(viewName);
        final AkibanInformationSchema newAIS = aisCloner.clone(oldAIS);
        newAIS.removeView(viewName);
        final String schemaName = viewName.getSchemaName();
        saveAISChange(session, newAIS, Collections.singleton(schemaName));
    }

    /** Add the Sequence to the current AIS */
    @Override
    public void createSequence(final Session session, final Sequence sequence) {
        checkSequenceName(session, sequence.getSequenceName(), false);
        AISMerge merge = AISMerge.newForOther(aisCloner, getNameGenerator(session), getAISForChange(session));
        AkibanInformationSchema newAIS = merge.mergeSequence(sequence);
        saveAISChange(session, newAIS, Collections.singleton(sequence.getSchemaName()));
    }

    /** Drop the given sequence from the current AIS. */
    @Override
    public void dropSequence(Session session, Sequence sequence) {
        checkSequenceName(session, sequence.getSequenceName(), true);
        AkibanInformationSchema oldAIS = getAISForChange(session);
        AkibanInformationSchema newAIS = removeTablesFromAIS(oldAIS,
                                                             Collections.<TableName>emptyList(),
                                                             Collections.singleton(sequence.getSequenceName()));
        saveAISChange(session, newAIS, Collections.singleton(sequence.getSchemaName()));
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
        createSQLJJarCommon(session, sqljJar, false, false);
    }

    @Override
    public void replaceSQLJJar(Session session, SQLJJar sqljJar) {
        createSQLJJarCommon(session, sqljJar, false, true);
    }

    @Override
    public void dropSQLJJar(Session session, TableName jarName) {
        dropSQLJJarCommon(session, jarName, false);
    }

    @Override
    public void registerSystemRoutine(final Routine routine) {
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    createRoutineCommon(session, routine, true, false);
                }
            });
        }
    }

    @Override
    public void unRegisterSystemRoutine(final TableName routineName) {
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    dropRoutineCommon(session, routineName, true);
                }
            });
        }
    }

    @Override
    public void registerSystemSQLJJar(final SQLJJar sqljJar) {
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    createSQLJJarCommon(session, sqljJar, true, false);
                }
            });
        }
    }

    @Override
    public void unRegisterSystemSQLJJar(final TableName jarName) {
        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    dropSQLJJarCommon(session, jarName, true);
                }
            });
        }
    }

    @Override
    public Set<String> getTreeNames(Session session) {
        return getNameGenerator(session).getStorageNames();
    }

    @Override
    public void setSecurityService(SecurityService securityService) {
        this.securityService = securityService;
    }

    @Override
    public TypesRegistry getTypesRegistry() {
        return typesRegistryService.getTypesRegistry();
    }

    @Override
    public TypesTranslator getTypesTranslator() {
        return typesTranslator;
    }

    @Override
    public StorageFormatRegistry getStorageFormatRegistry() {
        return storageFormatRegistry;
    }

    @Override
    public AISCloner getAISCloner() {
        return aisCloner;
    }

    //
    // Internal methods
    //

    private AkibanInformationSchema getAISForChange(Session session) {
        return getAISForChange(session, true);
    }

    private AkibanInformationSchema getAISForChange(Session session, boolean isOnlineAllowed) {
        // Rejected if not allowed
        OnlineSession onlineSession = getOnlineSession(session, isOnlineAllowed ? null : isOnlineAllowed);
        return (onlineSession != null) ? getOnlineAIS(session) : getSessionAIS(session);
    }

    private TableName createTableCommon(Session session, Table newTable, boolean isInternal,
                                        Integer version, boolean memoryTable) {
        final TableName newName = newTable.getName();
        checkTableName(session, newName, false, isInternal);
        AISInvariants.checkJoinTo(newTable.getParentJoin(), newName, isInternal);

        AkibanInformationSchema oldAIS = getAISForChange(session, !isInternal);
        AISMerge merge = AISMerge.newForAddTable(aisCloner, getNameGenerator(session), oldAIS, newTable);
        merge.merge();
        AkibanInformationSchema newAIS = merge.getAIS();
        Table mergedTable = newAIS.getTable(newName);

        ensureUuids(mergedTable);

        if(version == null) {
            version = 0; // New user or memory table
        }
        mergedTable.setVersion(version);
        newTableVersions(session, Collections.singletonMap(mergedTable.getTableId(), version));

        assignNewOrdinal(mergedTable);

        if(memoryTable) {
            // Memory only table changed, no reason to re-serialize
            assert mergedTable.hasMemoryTableFactory();
            unStoredAISChange(session, newAIS);
        } else {
            saveAISChange(session, newAIS, Collections.singleton(newName.getSchemaName()));
        }
        return newName;
    }

    private static void ensureUuids(Table newTable) {
        if (newTable.getUuid() == null)
            newTable.setUuid(UUID.randomUUID());
        for (Column newColumn : newTable.getColumnsIncludingInternal()) {
            if (newColumn.getUuid() == null)
                newColumn.setUuid(UUID.randomUUID());
        }
    }

    private void dropTableCommon(Session session, TableName tableName, final DropBehavior dropBehavior,
                                 final boolean isInternal, final boolean mustBeMemory) {
        checkTableName(session, tableName, true, isInternal);
        final AkibanInformationSchema oldAIS = getAISForChange(session, !isInternal);
        final Table table = oldAIS.getTable(tableName);

        final List<TableName> tables = new ArrayList<>();
        final Set<String> schemas = new HashSet<>();
        final List<Integer> tableIDs = new ArrayList<>();
        final Set<TableName> sequences = new HashSet<>();

        // Collect all tables in branch below this point
        table.visit(new AbstractVisitor() {
            @Override
            public void visit(Table table) {
                if(mustBeMemory && !table.hasMemoryTableFactory()) {
                    throw new IllegalArgumentException("Cannot un-register non-memory table");
                }

                if((dropBehavior == DropBehavior.RESTRICT) && !table.getChildJoins().isEmpty()) {
                    throw new ReferencedTableException (table);
                }

                TableName name = table.getName();
                tables.add(name);
                schemas.add(name.getSchemaName());
                tableIDs.add(table.getTableId());
                for (Column column : table.getColumnsIncludingInternal()) {
                    if (column.getIdentityGenerator() != null) {
                        sequences.add(column.getIdentityGenerator().getSequenceName());
                    }
                }
            }
        });

        final AkibanInformationSchema newAIS = removeTablesFromAIS(oldAIS, tables, sequences);

        for(Integer tableID : tableIDs) {
            clearTableStatus(session, oldAIS.getTable(tableID));
        }

        if(table.hasMemoryTableFactory()) {
            unStoredAISChange(session, newAIS);
        } else {
            saveAISChange(session, newAIS, schemas, tableIDs);
        }
    }

    private void createRoutineCommon(Session session, Routine routine,
                                     boolean inSystem, boolean replaceExisting) {
        final AkibanInformationSchema oldAIS = getAISForChange(session, !inSystem);
        checkSystemSchema(routine.getName(), inSystem);
        if (!replaceExisting && (oldAIS.getRoutine(routine.getName()) != null))
            throw new DuplicateRoutineNameException(routine.getName());
        // This may not be the generation that newAIS will receive,
        // but it will still be > than any previous routine, and in
        // particular any by the same name, whether replaced here or
        // dropped earlier.
        routine.setVersion(oldAIS.getGeneration() + 1);
        final AkibanInformationSchema newAIS = AISMerge.mergeRoutine(aisCloner, oldAIS, routine);
        if (inSystem)
            unStoredAISChange(session, newAIS);
        else {
            final String schemaName = routine.getName().getSchemaName();
            saveAISChange(session, newAIS, Collections.singleton(schemaName));
        }
    }

    private void dropRoutineCommon(Session session, TableName routineName, boolean inSystem) {
        final AkibanInformationSchema oldAIS = getAISForChange(session, !inSystem);
        checkSystemSchema(routineName, inSystem);
        Routine routine = oldAIS.getRoutine(routineName);
        if (routine == null)
            throw new NoSuchRoutineException(routineName);
        final AkibanInformationSchema newAIS = aisCloner.clone(oldAIS);
        routine = newAIS.getRoutine(routineName);
        newAIS.removeRoutine(routineName);
        if (routine.getSQLJJar() != null)
            routine.getSQLJJar().removeRoutine(routine); // Keep accurate in memory.
        if (inSystem)
            unStoredAISChange(session, newAIS);
        else {
            final String schemaName = routineName.getSchemaName();
            saveAISChange(session, newAIS, Collections.singleton(schemaName));
        }
    }

    private void createSQLJJarCommon(Session session, SQLJJar sqljJar,
                                     boolean inSystem, boolean replace) {
        final AkibanInformationSchema oldAIS = getAISForChange(session);
        checkSystemSchema(sqljJar.getName(), inSystem);
        if (replace) {
            if (oldAIS.getSQLJJar(sqljJar.getName()) == null)
                throw new NoSuchSQLJJarException(sqljJar.getName());
        }
        else {
            if (oldAIS.getSQLJJar(sqljJar.getName()) != null)
                throw new DuplicateSQLJJarNameException(sqljJar.getName());
        }
        sqljJar.setVersion(oldAIS.getGeneration() + 1);
        final AkibanInformationSchema newAIS;
        if (replace) {
            newAIS = aisCloner.clone(oldAIS);
            // Changing old state rather than actually replacing saves having to find
            // referencing routines, possibly in other schemas.
            final SQLJJar oldJar = newAIS.getSQLJJar(sqljJar.getName());
            assert (oldJar != null);
            oldJar.setURL(sqljJar.getURL());
            oldJar.setVersion(sqljJar.getVersion());
        }
        else {
            newAIS = AISMerge.mergeSQLJJar(aisCloner, oldAIS, sqljJar);
        }
        if (inSystem) {
            unStoredAISChange(session, newAIS);
        }
        else {
            final String schemaName = sqljJar.getName().getSchemaName();
            saveAISChange(session, newAIS, Collections.singleton(schemaName));
        }
    }

    private void dropSQLJJarCommon(Session session, TableName jarName,
                                   boolean inSystem) {
        final AkibanInformationSchema oldAIS = getAISForChange(session);
        checkSystemSchema(jarName, inSystem);
        SQLJJar sqljJar = oldAIS.getSQLJJar(jarName);
        if (sqljJar == null)
            throw new NoSuchSQLJJarException(jarName);
        if (!sqljJar.getRoutines().isEmpty())
            throw new ReferencedSQLJJarException(sqljJar);
        final AkibanInformationSchema newAIS = aisCloner.clone(oldAIS);
        newAIS.removeSQLJJar(jarName);
        if (inSystem) {
            unStoredAISChange(session, newAIS);
        }
        else {
            final String schemaName = jarName.getSchemaName();
            saveAISChange(session, newAIS, Collections.singleton(schemaName));
        }
    }

    private AkibanInformationSchema removeSchemaFromAIS(AkibanInformationSchema oldAIS, final String schemaName) {
        return aisCloner.clone(oldAIS,
                new ProtobufWriter.WriteSelector() {
                    @Override
                    public Columnar getSelected(Columnar columnar) {
                        return null;
                    }

                    @Override
                    public boolean isSelected(Group group) {
                        return !group.getSchemaName().equals(schemaName);
                    }

                    @Override
                    public boolean isSelected(Join parentJoin) {
                        return !parentJoin.getGroup().getSchemaName().equals(schemaName);
                    }

                    @Override
                    public boolean isSelected(Index index) {
                        return !index.getSchemaName().equals(schemaName);
                    }

                    @Override
                    public boolean isSelected(Sequence sequence) {
                        return !sequence.getSchemaName().equals(schemaName);
                    }

                    @Override
                    public boolean isSelected(Routine routine) {
                        return !routine.getName().getSchemaName().equals(schemaName);
                    }

                    @Override
                    public boolean isSelected(SQLJJar sqljJar) {
                        return !sqljJar.getName().getSchemaName().equals(schemaName);
                    }

                    @Override
                    public boolean isSelected(ForeignKey foreignKey) {
                        return !foreignKey.getReferencingTable().getGroup().getSchemaName().equals(schemaName);
                    }
                });
    }

    /** Construct a new AIS from {@code oldAIS} without {@code tableNames} or {@code sequences}. */
    private AkibanInformationSchema removeTablesFromAIS(AkibanInformationSchema oldAIS,
                                                        final List<TableName> tableNames,
                                                        final Set<TableName> sequences) {
        return aisCloner.clone(
                oldAIS,
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
                    public boolean isSelected(ForeignKey foreignKey) {
                        return !tableNames.contains(foreignKey.getReferencingTable().getName()) &&
                               !tableNames.contains(foreignKey.getReferencedTable().getName());
                    }

                    @Override
                    public boolean isSelected(Sequence sequence) {
                        return !sequences.contains(sequence.getSequenceName());
                    }
                }
        );
    }

    private static void assignNewOrdinal(final Table newTable) {
        assert newTable.getOrdinal() == null : newTable + ": " + newTable.getOrdinal();
        MaxOrdinalVisitor visitor = new MaxOrdinalVisitor();
        newTable.getGroup().visit(visitor);
        newTable.setOrdinal(visitor.maxOrdinal + 1);
    }

    private static void checkSystemSchema(TableName tableName, boolean shouldBeSystem) {
        final boolean inSystem = tableName.inSystemSchema();
        
        if(shouldBeSystem && !inSystem) {
            throw new IllegalArgumentException("Table required to be in "+TableName.INFORMATION_SCHEMA +" schema");
        }
        if(!shouldBeSystem && inSystem) {
            throw new ProtectedTableDDLException(tableName);
        }
    }

    private void checkTableName(Session session, TableName tableName, boolean shouldExist, boolean inIS) {
        checkSystemSchema(tableName, inIS);
        if (!inIS && (securityService != null) &&
            !securityService.isAccessible(session, tableName.getSchemaName())) {
            throw new ProtectedTableDDLException(tableName);
        }
        final boolean tableExists = getAISForChange(session, !inIS).getTable(tableName) != null;
        if(shouldExist && !tableExists) {
            throw new NoSuchTableException(tableName);
        }
        if(!shouldExist && tableExists) {
            throw new DuplicateTableNameException(tableName);
        }
    }

    private void checkSequenceName(Session session, TableName sequenceName, boolean shouldExist) {
        checkSystemSchema (sequenceName, false);
        final boolean exists = getAISForChange(session).getSequence(sequenceName) != null;
        if (shouldExist && !exists) {
            throw new NoSuchSequenceException(sequenceName);
        }
        if (!shouldExist && exists) {
            throw new DuplicateSequenceNameException(sequenceName);
        }
    }

    /** Change affecting the given schemas. Does not affect any table in an incompatible way (e.g. metadata only). */
    private void saveAISChange(Session session,
                               AkibanInformationSchema newAIS,
                               Collection<String> schemas) {
        saveAISChange(session, newAIS, schemas, Collections.<Integer>emptyList());
    }

    /** Change affecting the given schemas and tables. **/
    private void saveAISChange(Session session,
                               AkibanInformationSchema newAIS,
                               Collection<String> schemas,
                               Collection<Integer> tableIDs) {
        assert schemas != null;
        assert tableIDs != null;
        AkibanInformationSchema oldAIS = getAISForChange(session);
        OnlineSession onlineSession = getOnlineSession(session, null);
        OnlineCache onlineCache = getOnlineCache(session, oldAIS);

        for(Integer tid : tableIDs) {
            Long curOwner = onlineCache.tableToOnline.get(tid);
            if((curOwner != null) && (onlineSession == null || !curOwner.equals(onlineSession.id))) {
                throw new OnlineDDLInProgressException(tid);
            }
        }

        for(String name : schemas) {
            Long curOwner = onlineCache.schemaToOnline.get(name);
            if((curOwner != null) && (onlineSession == null || !curOwner.equals(onlineSession.id))) {
                throw new OnlineDDLInProgressException(name);
            }
        }

        bumpTableVersions(session, newAIS, tableIDs);

        if(onlineSession != null) {
            onlineSession.schemaNames.addAll(schemas);
            onlineSession.tableIDs.addAll(tableIDs);
            storedOnlineChange(session, onlineSession, newAIS, schemas);
        } else {
            storedAISChange(session, newAIS, schemas);
        }
    }


    //
    // Internal classes
    //

    private static class MaxOrdinalVisitor extends AbstractVisitor {
        public int maxOrdinal = 0;

        @Override
        public void visit(Table table) {
            Integer ordinal = table.getOrdinal();
            if((ordinal != null) && (ordinal > maxOrdinal)) {
                maxOrdinal = ordinal;
            }
        }
    }

    protected static class OnlineSession
    {
        public final long id;
        /** Schemas affected by this session. */
        public final Set<String> schemaNames;
        /** Tables affected by this session. */
        public final Set<Integer> tableIDs;

        public OnlineSession(long id) {
            this.id = id;
            this.schemaNames = new HashSet<>();
            this.tableIDs = new HashSet<>();
        }
    }

    protected static class OnlineCache
    {
        public final Map<String,Long> schemaToOnline = new HashMap<>();
        public final Map<Integer,Long> tableToOnline = new HashMap<>();
        public final Map<Long,AkibanInformationSchema> onlineToAIS = new HashMap<>();
        public final Multimap<Long,ChangeSet> onlineToChangeSets = HashMultimap.create();
    }

    protected static class ReadOnlyOnlineChangeState implements OnlineChangeState {
        private final AkibanInformationSchema ais;
        private final Collection<ChangeSet> changeSets;

        public ReadOnlyOnlineChangeState(AkibanInformationSchema ais, Collection<ChangeSet> changeSets) {
            assert ais.isFrozen();
            this.ais = ais;
            this.changeSets = Collections.unmodifiableCollection(changeSets);
        }

        @Override
        public AkibanInformationSchema getAIS() {
            return ais;
        }

        @Override
        public Collection<ChangeSet> getChangeSets() {
            return changeSets;
        }
    }

    private static final Callback REMOVE_ONLINE_SESSION_KEY_CALLBACK = new Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(ONLINE_SESSION_KEY);
        }
    };
}
