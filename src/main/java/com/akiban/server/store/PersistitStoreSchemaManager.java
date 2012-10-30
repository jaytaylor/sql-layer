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

package com.akiban.server.store;

import static com.akiban.server.service.tree.TreeService.SCHEMA_TREE_NAME;
import static com.akiban.qp.persistitadapter.PersistitAdapter.wrapPersistitException;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AISMerge;
import com.akiban.ais.model.AISTableNameChanger;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.SynchronizedNameGenerator;
import com.akiban.ais.model.View;
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.ChangedTableDescription;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.server.error.AISTooLargeException;
import com.akiban.server.error.DuplicateRoutineNameException;
import com.akiban.server.error.DuplicateSequenceNameException;
import com.akiban.server.error.DuplicateSQLJJarNameException;
import com.akiban.server.error.DuplicateTableNameException;
import com.akiban.server.error.DuplicateViewException;
import com.akiban.server.error.ISTableVersionMismatchException;
import com.akiban.server.error.JoinToProtectedTableException;
import com.akiban.server.error.NoSuchRoutineException;
import com.akiban.server.error.NoSuchSequenceException;
import com.akiban.server.error.NoSuchSQLJJarException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ProtectedTableDDLException;
import com.akiban.server.error.ReferencedTableException;
import com.akiban.server.error.ReferencedSQLJJarException;
import com.akiban.server.error.UndefinedViewException;
import com.akiban.server.error.UnsupportedMetadataTypeException;
import com.akiban.server.error.UnsupportedMetadataVersionException;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.GrowableByteBuffer;
import com.google.inject.Inject;

import com.persistit.Accumulator;
import com.persistit.Key;
import com.persistit.KeyFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.DDLGenerator;
import com.akiban.server.service.Service;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeVisitor;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;

/**
 * <p>
 * Storage as of v1.2.1 (05/2012), Protobuf bytes per schema:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS",(int)version,"schema1"</td>
 *         <td>byte[]</td>
 *         <td>Key is a pair of version (int, see {@link #PROTOBUF_PSSM_VERSION} for current) and schema name (string).<br>
 *             Value is as constructed by {@link ProtobufWriter}.
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS",(int)version,"schema2"</td>
 *         <td>...</td>
 *         <td>...</td>
 *     </tr>
 * </table>
 * <br>
 * <p>
 * Storage as of v0.4 (05/2011), MetaModel bytes per AIS:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byAIS"</td>
 *         <td>byte[]</td>
 *         <td>Value is as constructed by (now deleted) com.akiban.ais.metamodel.io.MessageTarget</td>
 *     </tr>
 * </table>
 * </p>
 * </p>
 */
public class PersistitStoreSchemaManager implements Service, SchemaManager {
    public static enum SerializationType {
        NONE,
        META_MODEL,
        PROTOBUF,
        UNKNOWN
    }

    private static enum GenValue { NEW, SNAPSHOT }
    private static enum GenMap { PUT_NEW, NO_PUT }

    public static final String MAX_AIS_SIZE_PROPERTY = "akserver.max_ais_size_bytes";
    public static final String SKIP_AIS_UPGRADE_PROPERTY = "akserver.skip_ais_upgrade";
    public static final SerializationType DEFAULT_SERIALIZATION = SerializationType.PROTOBUF;

    public static final String DEFAULT_CHARSET = "akserver.default_charset";
    public static final String DEFAULT_COLLATION = "akserver.default_collation";

    private static final String AIS_KEY_PREFIX = "by";
    private static final String AIS_METAMODEL_PARENT_KEY = AIS_KEY_PREFIX + "AIS";
    private static final String AIS_PROTOBUF_PARENT_KEY = AIS_KEY_PREFIX + "PBAIS";
    private static final String DELAYED_TREE_KEY = "delayedTree";

    private static final int SCHEMA_GEN_ACCUM_INDEX = 0;
    private static final Accumulator.Type SCHEMA_GEN_ACCUM_TYPE = Accumulator.Type.SEQ;

    // Changed from 1 to 2 due to incompatibility related to index row changes (see bug 985007)
    private static final int PROTOBUF_PSSM_VERSION = 2;

    private static final Session.Key<AkibanInformationSchema> SESSION_AIS_KEY = Session.Key.named("AIS_KEY");

    private static final String CREATE_SCHEMA_FORMATTER = "create schema if not exists `%s`;";
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStoreSchemaManager.class.getName());

    private final SessionService sessionService;
    private final TreeService treeService;
    private final ConfigurationService config;
    private final TransactionService txnService;
    private RowDefCache rowDefCache;
    private int maxAISBufferSize;
    private boolean skipAISUpgrade;
    private SerializationType serializationType = SerializationType.NONE;
    private NameGenerator nameGenerator;
    private AtomicLong delayedTreeIDGenerator;
    private SortedMap<Long,AkibanInformationSchema> aisMap;
    private ReentrantReadWriteLock aisMapLock;

    @Inject
    public PersistitStoreSchemaManager(ConfigurationService config, SessionService sessionService,
                                       TreeService treeService, TransactionService txnService) {
        this.config = config;
        this.sessionService = sessionService;
        this.treeService = treeService;
        this.txnService = txnService;
    }

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
    public Collection<Index> createIndexes(Session session, Collection<? extends Index> indexesToAdd) {
        AISMerge merge = AISMerge.newForAddIndex(nameGenerator, getAis(session));
        Set<String> schemas = new HashSet<String>();
        Collection<Index> newIndexes = new ArrayList<Index>(indexesToAdd.size());
        for(Index proposed : indexesToAdd) {
            Index newIndex = merge.mergeIndex(proposed);
            newIndexes.add(newIndex);
            schemas.add(DefaultNameGenerator.schemaNameForIndex(newIndex));
        }
        merge.merge();
        saveAISChangeWithRowDefs(session, merge.getAIS(), schemas);
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

        final Set<String> schemas = new HashSet<String>();
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

        Set<String> schemas = new HashSet<String>();
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
        }

        AISMerge merge = AISMerge.newForModifyTable(nameGenerator, getAis(session), alteredTables);
        merge.merge();
        AkibanInformationSchema newAIS = merge.getAIS();

        for(ChangedTableDescription desc : alteredTables) {
            if(desc.isNewGroup()) {
                UserTable table = newAIS.getUserTable(desc.getNewName());
                try {
                    treeService.getTableStatusCache().setOrdinal(table.getTableId(), 0);
                } catch(PersistitException e) {
                    throw wrapPersistitException(session, e);
                }
            }
        }

        saveAISChangeWithRowDefs(session, newAIS, schemas);
    }

    private void dropTableCommon(Session session, TableName tableName, final DropBehavior dropBehavior,
                                 final boolean isInternal, final boolean mustBeMemory) {
        checkTableName(session, tableName, true, isInternal);
        final UserTable table = getAis(session).getUserTable(tableName);
        assert table != null : tableName + " is a GroupTable";

        final List<TableName> tables = new ArrayList<TableName>();
        final Set<String> schemas = new HashSet<String>();
        final List<Integer> tableIDs = new ArrayList<Integer>();
        final Set<TableName> sequences = new HashSet<TableName>();

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

        final AkibanInformationSchema newAIS = removeTablesFromAIS(session, tables, sequences);
        saveAISChangeWithRowDefs(session, newAIS, schemas);
        //deleteTableStatuses(tableIDs);
    }

    // Can no longer be called on drop now that AIS is transactional.
    // TableStatus are around as long as needed (attached to RowDefs) and IDs don't get reused until restart
    private void deleteTableStatuses(List<Integer> tableIDs) throws PersistitException {
        for(Integer id : tableIDs) {
            treeService.getTableStatusCache().drop(id);
            nameGenerator.removeTableID(id);
        }
    }

    @Override
    public TableDefinition getTableDefinition(Session session, TableName tableName) {
        final Table table = getAis(session).getTable(tableName);
        if(table == null) {
            throw new NoSuchTableException(tableName);
        }
        final String ddl = new DDLGenerator().createTable(table);
        return new TableDefinition(table.getTableId(), tableName, ddl);
    }

    @Override
    public SortedMap<String, TableDefinition> getTableDefinitions(Session session, String schemaName) {
        final SortedMap<String, TableDefinition> result = new TreeMap<String, TableDefinition>();
        final DDLGenerator gen = new DDLGenerator();
        for(UserTable table : getAis(session).getUserTables().values()) {
            final TableName name = table.getName();
            if(name.getSchemaName().equals(schemaName)) {
                final String ddl = gen.createTable(table);
                final TableDefinition def = new TableDefinition(table.getTableId(), name, ddl);
                result.put(name.getTableName(), def);
            }
        }
        return result;
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

    @Override
    public void createRoutine(Session session, Routine routine) {
        createRoutineCommon(session, routine, false);
    }

    @Override
    public void dropRoutine(Session session, TableName routineName) {
        dropRoutineCommon(session, routineName, false);
    }

    @Override
    public void registerSystemRoutine(final Routine routine) {
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                createRoutineCommon(session, routine, true);
            }
        });
    }

    @Override
    public void unRegisterSystemRoutine(final TableName routineName) {
        transactionally(sessionService.createSession(), new ThrowingRunnable() {
            @Override
            public void run(Session session) throws PersistitException {
                dropRoutineCommon(session, routineName, false);
            }
        });
    }

    private void createRoutineCommon(Session session, Routine routine, boolean inSystem) {
        final AkibanInformationSchema oldAIS = getAis(session);
        checkSystemSchema(routine.getName(), inSystem);
        if (oldAIS.getRoutine(routine.getName()) != null)
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
    public AkibanInformationSchema getAis(Session session) {
        AkibanInformationSchema local = session.get(SESSION_AIS_KEY);
        if(local != null) {
            return local;
        }

        final long generation = getGenerationSnapshot(session);
        sharedMapClaim();
        try {
            local = aisMap.get(generation);
        } finally {
            sharedMapRelease();
        }

        // If still null then AIS wasn't in map. Need to reload from disk.
        // Should be 1) very rare and 2) fairly quick, so just do it under write lock to avoid duplicate work/entries
        if(local == null) {
            exclusiveMapClaim();
            try {
                // Look again while under exclusive
                local = aisMap.get(generation);
                if(local == null) {
                    try {
                        local = loadAISFromStorage(session, GenValue.SNAPSHOT, GenMap.PUT_NEW);
                        buildRowDefCache(local);
                    } catch(PersistitException e) {
                        throw wrapPersistitException(session, e);
                    }
                }
            } finally {
                exclusiveMapRelease();
            }
        }

        session.put(SESSION_AIS_KEY, local);
        txnService.addEndCallback(session, CLEAR_SESSION_KEY_CALLBACK);
        return local;
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
                        GroupIndex gi = (GroupIndex)index;
                        for(IndexColumn icol : gi.getKeyColumns()) {
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

    @Override
    public List<String> schemaStrings(Session session, boolean withISTables) {
        final AkibanInformationSchema ais = getAis(session);
        final DDLGenerator generator = new DDLGenerator();
        final List<String> ddlList = new ArrayList<String>();
        for(Schema schema : ais.getSchemas().values()) {
            if(!withISTables && 
               (TableName.INFORMATION_SCHEMA.equals(schema.getName()) ||
                TableName.SYS_SCHEMA.equals(schema.getName()) ||
                TableName.SQLJ_SCHEMA.equals(schema.getName()))) {
                continue;
            }
            ddlList.add(String.format(CREATE_SCHEMA_FORMATTER, schema.getName()));
            for(UserTable table : schema.getUserTables().values()) {
                ddlList.add(generator.createTable(table));
            }
        }
        return ddlList;
    }

    /** Add the Sequence to the current AIS */
    @Override
    public void createSequence(Session session, Sequence sequence) {
        checkSequenceName(session, sequence.getSequenceName(), false);
        AkibanInformationSchema newAIS = AISMerge.mergeSequence(this.getAis(session), sequence);
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(sequence.getSchemaName()));
        try {
            sequence.setStartWithAccumulator(treeService);
        } catch (PersistitException e) {
            LOG.error("Setting sequence starting value for sequence {} failed", sequence.getSequenceName().getDescription());
            throw wrapPersistitException(session, e);
        }
    }
    
    /** Drop the given sequence from the current AIS. */
    @Override
    public void dropSequence(Session session, Sequence sequence) {
        checkSequenceName(session, sequence.getSequenceName(), true);
        List<TableName> emptyList = new ArrayList<TableName>(0);
        final AkibanInformationSchema newAIS = removeTablesFromAIS(session, emptyList, Collections.singleton(sequence.getSequenceName()));
        saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(sequence.getSchemaName()));
    }

    @Override
    public boolean treeRemovalIsDelayed() {
        return true;
    }

    @Override
    public void treeWasRemoved(Session session, String schema, String treeName) {
        if(!treeRemovalIsDelayed()) {
            nameGenerator.removeTreeName(treeName);
            return;
        }

        LOG.debug("Delaying removal of tree (until next restart): {}", treeName);
        Exchange ex = schemaTreeExchange(session, schema);
        try {
            long id = delayedTreeIDGenerator.incrementAndGet();
            long timestamp = System.currentTimeMillis();
            ex.clear().append(DELAYED_TREE_KEY).append(id).append(timestamp);
            ex.getValue().setStreamMode(true);
            ex.getValue().put(schema);
            ex.getValue().put(treeName);
            ex.getValue().setStreamMode(false);
            ex.store();
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    public void start() {
        rowDefCache = new RowDefCache(treeService.getTableStatusCache());
        skipAISUpgrade = Boolean.parseBoolean(config.getProperty(SKIP_AIS_UPGRADE_PROPERTY));
        maxAISBufferSize = Integer.parseInt(config.getProperty(MAX_AIS_SIZE_PROPERTY));
        if(maxAISBufferSize < 0) {
            LOG.warn("Clamping property "+MAX_AIS_SIZE_PROPERTY+" to 0");
            maxAISBufferSize = 0;
        }
        AkibanInformationSchema.setDefaultCharsetAndCollation(config.getProperty(DEFAULT_CHARSET),
                                                              config.getProperty(DEFAULT_COLLATION));

        this.aisMap = new TreeMap<Long, AkibanInformationSchema>();
        this.aisMapLock = new ReentrantReadWriteLock();

        AkibanInformationSchema newAIS = transactionally(
                sessionService.createSession(),
                new ThrowingCallable<AkibanInformationSchema>() {
                    @Override
                    public AkibanInformationSchema runAndReturn(Session session) throws PersistitException {
                        // Unrelated to loading, but fine time to do it
                        cleanupDelayedTrees(session);

                        AkibanInformationSchema ais = loadAISFromStorage(session, GenValue.SNAPSHOT, GenMap.PUT_NEW);
                        if(!skipAISUpgrade) {
                            // Upgrade goes here if we ever need another one
                        } else {
                            //LOG.warn("Skipping AIS upgrade");
                        }
                        buildRowDefCache(ais);
                        return ais;
                    }
                }
        );

        this.nameGenerator = SynchronizedNameGenerator.wrap(new DefaultNameGenerator(newAIS));
        this.delayedTreeIDGenerator = new AtomicLong();
    }

    @Override
    public void stop() {
        this.rowDefCache = null;
        this.maxAISBufferSize = 0;
        this.skipAISUpgrade = false;
        this.serializationType = SerializationType.NONE;
        this.nameGenerator = null;
        this.delayedTreeIDGenerator = null;
        this.aisMap = null;
        this.aisMapLock = null;
    }

    @Override
    public void crash() {
        stop();
    }

    private AkibanInformationSchema loadAISFromStorage(final Session session, GenValue genValue, GenMap genMap) throws PersistitException {
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        treeService.visitStorage(
                session,
                new TreeVisitor() {
                    @Override
                    public void visit(Exchange ex) throws PersistitException{
                        SerializationType typeForVolume = detectSerializationType(session, ex);
                        switch(typeForVolume) {
                            case NONE:
                                // Empty tree, nothing to do
                            break;
                            case PROTOBUF:
                                checkAndSetSerialization(typeForVolume);
                                loadProtobuf(ex, newAIS);
                            break;
                            default:
                                throw new UnsupportedMetadataTypeException(typeForVolume.name());
                        }
                    }
                },
                SCHEMA_TREE_NAME
        );
        validateAndFreeze(session, newAIS, genValue, genMap);
        return newAIS;
    }

    private static void loadProtobuf(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        ProtobufReader reader = new ProtobufReader(newAIS);
        Key key = ex.getKey();
        ex.clear().append(AIS_KEY_PREFIX);
        KeyFilter filter = new KeyFilter().append(KeyFilter.simpleTerm(AIS_PROTOBUF_PARENT_KEY));
        while(ex.traverse(Key.Direction.GT, filter, Integer.MAX_VALUE)) {
            if(key.getDepth() != 3) {
                throw new IllegalStateException("Unexpected " + AIS_PROTOBUF_PARENT_KEY + " format: " + key);
            }

            key.indexTo(1);
            int storedVersion = key.decodeInt();
            String storedSchema = key.decodeString();
            if(storedVersion != PROTOBUF_PSSM_VERSION) {
                LOG.debug("Unsupported version {} for schema {}", storedVersion, storedSchema);
                throw new UnsupportedMetadataVersionException(PROTOBUF_PSSM_VERSION, storedVersion);
            }

            byte[] storedAIS = ex.getValue().getByteArray();
            GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }

        reader.loadAIS();

        // ProtobufWriter does not save group tables (by design) so generate columns and indexes
        AISBuilder builder = new AISBuilder(newAIS);
        builder.groupingIsComplete();
    }

    private void buildRowDefCache(AkibanInformationSchema newAis) throws PersistitException {
        treeService.getTableStatusCache().detachAIS();
        // This create|verifies the trees exist for indexes & tables
        rowDefCache.setAIS(newAis);
        // This creates|verifies the trees exist for sequences
        sequenceTrees(newAis);
    }

    private void sequenceTrees (final AkibanInformationSchema newAis) throws PersistitException {
        for (Sequence sequence : newAis.getSequences().values()) {
            LOG.debug("registering sequence: " + sequence.getSequenceName() + " with tree name: " + sequence.getTreeName());
            // treeCache == null -> loading from start or creating a new sequence
            if (sequence.getTreeCache() == null) {
                treeService.populateTreeCache(sequence);
            }
        }
    }

    private void saveProtobuf(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, final String schema)
            throws PersistitException {
        final ProtobufWriter.WriteSelector selector;
        if(TableName.INFORMATION_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                @Override
                public Columnar getSelected(Columnar columnar) {
                    if(columnar.isTable() && ((UserTable)columnar).hasMemoryTableFactory()) {
                        return null;
                    }
                    return columnar;
                }
            };
        } else if(TableName.SYS_SCHEMA.equals(schema) ||
                  TableName.SQLJ_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                    @Override
                    public boolean isSelected(Routine routine) {
                        return false;
                    }
            };
        } else {
            selector = new ProtobufWriter.SingleSchemaSelector(schema);
        }

        ex.clear().append(AIS_PROTOBUF_PARENT_KEY).append(PROTOBUF_PSSM_VERSION).append(schema);
        if(newAIS.getSchema(schema) != null) {
            buffer.clear();
            new ProtobufWriter(buffer, selector).save(newAIS);
            buffer.flip();

            ex.getValue().clear().putByteArray(buffer.array(), buffer.position(), buffer.limit());
            ex.store();
        } else {
            ex.remove();
        }
    }

    private void validateAndFreeze(Session session, AkibanInformationSchema newAIS, GenValue genValue, GenMap genMap) {
        session.remove(SESSION_AIS_KEY); // Remove old cache

        newAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary(); // TODO: Often redundant, cleanup
        long generation = (genValue == GenValue.NEW) ? getNextGeneration(session) : getGenerationSnapshot(session);
        newAIS.setGeneration(generation);
        newAIS.freeze();

        if(genMap == GenMap.PUT_NEW) {
            saveNewAISInMap(newAIS);
        }
    }

    private void saveNewAISInMap(AkibanInformationSchema ais) {
        long generation = ais.getGeneration();
        aisMapLock.writeLock().lock();
        try {
            if(aisMap.containsKey(generation)) {
                throw new IllegalStateException("Expected new generation: " + generation);
            }
            aisMap.put(generation, ais);
        } finally {
            aisMapLock.writeLock().unlock();
        }
    }

    /**
     * Internal helper for saving an AIS change to storage. This includes create, delete, alter, etc.
     *
     * @param session Session to run under
     * @param newAIS The new AIS to store on disk
     * @param schemaNames The schemas affected by the change
     */
    private void saveAISChangeWithRowDefs(Session session, AkibanInformationSchema newAIS, Collection<String> schemaNames) {
        int maxSize = maxAISBufferSize == 0 ? Integer.MAX_VALUE : maxAISBufferSize;
        GrowableByteBuffer byteBuffer = new GrowableByteBuffer(4096, 4096, maxSize);
        Exchange ex = null;
        try {
            validateAndFreeze(session, newAIS, GenValue.NEW, GenMap.PUT_NEW);
            for(String schema : schemaNames) {
                ex = schemaTreeExchange(session, schema);
                checkAndSerialize(ex, byteBuffer, newAIS, schema);
                treeService.releaseExchange(session, ex);
                ex = null;
            }
            buildRowDefCache(newAIS);
        } catch(BufferOverflowException e) {
            throw new AISTooLargeException(maxSize);
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } finally {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
        }
    }

    private void unSavedAISChangeWithRowDefs(Session session, AkibanInformationSchema newAIS) {
        try {
            validateAndFreeze(session, newAIS, GenValue.NEW, GenMap.PUT_NEW);
            buildRowDefCache(newAIS);
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    public static SerializationType detectSerializationType(Session session, Exchange ex) {
        try {
            SerializationType type = SerializationType.NONE;

            // Simple heuristic to determine which style AIS storage we have
            boolean hasMetaModel = false;
            boolean hasProtobuf = false;
            boolean hasUnknown = false;

            ex.clear().append(AIS_KEY_PREFIX);
            while(ex.next(true)) {
                if(ex.getKey().decodeType() != String.class) {
                    break;
                }
                String k = ex.getKey().decodeString();
                if(!k.startsWith(AIS_KEY_PREFIX)) {
                    break;
                }
                if(k.equals(AIS_PROTOBUF_PARENT_KEY)) {
                    hasProtobuf = true;
                } else if(k.equals(AIS_METAMODEL_PARENT_KEY)) {
                    hasMetaModel = true;
                } else {
                    hasUnknown = true;
                }
            }

            if(hasMetaModel && hasProtobuf) {
                throw new IllegalStateException("Both AIS and Protobuf serializations");
            } else if(hasMetaModel) {
                type = SerializationType.META_MODEL;
            } else if(hasProtobuf) {
                type = SerializationType.PROTOBUF;
            } else if(hasUnknown) {
                type = SerializationType.UNKNOWN;
            }

            return type;
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private void checkAndSetSerialization(SerializationType newSerializationType) {
        if((serializationType != SerializationType.NONE) && (serializationType != newSerializationType)) {
            throw new IllegalStateException("Mixed serialization types: " + serializationType + " vs " + newSerializationType);
        }
        serializationType = newSerializationType;
    }

    // Public for tests
    public void cleanupDelayedTrees(final Session session) throws PersistitException {
        treeService.visitStorage(
                session,
                new TreeVisitor() {
                    @Override
                    public void visit(final Exchange ex) throws PersistitException {
                        ex.clear().append(DELAYED_TREE_KEY);
                        KeyFilter filter = new KeyFilter().append(KeyFilter.simpleTerm(DELAYED_TREE_KEY));
                        while(ex.traverse(Key.Direction.GT, filter, Integer.MAX_VALUE)) {
                            ex.getKey().indexTo(1);     // skip delayed key
                            ex.getKey().decodeLong();   // skip id
                            long timestamp = ex.getKey().decodeLong();
                            ex.getValue().setStreamMode(true);
                            String schema = ex.getValue().getString();
                            String treeName = ex.getValue().getString();
                            ex.getValue().setStreamMode(false);
                            ex.remove();

                            LOG.debug("Removing delayed tree {} from timestamp {}", treeName, timestamp);
                            TreeLink link = treeService.treeLink(schema, treeName);
                            Exchange ex2 = treeService.getExchange(session, link);
                            ex2.removeTree();
                            treeService.releaseExchange(session, ex2);

                            // Keep consistent if called during runtime
                            if(nameGenerator != null) {
                                nameGenerator.removeTreeName(treeName);
                            }
                        }
                    }
                },
                SCHEMA_TREE_NAME
        );
    }

    // Package for tests
    void clearAISMap() {
        aisMapLock.writeLock().lock();
        try {
            aisMap.clear();
        } finally {
            aisMapLock.writeLock().unlock();
        }
    }

    private Exchange schemaTreeExchange(Session session, String schema) {
        TreeLink link = treeService.treeLink(schema, SCHEMA_TREE_NAME);
        return treeService.getExchange(session, link);
    }

    /**
     * @return Current serialization type
     */
    public SerializationType getSerializationType() {
        return serializationType;
    }

    @Override
    public Set<String> getTreeNames() {
        return nameGenerator.getTreeNames();
    }

    @Override
    public long getOldestActiveAISGeneration() {
        sharedMapClaim();
        try {
            if(aisMap.isEmpty()) {
                return Long.MIN_VALUE;
            }
            return aisMap.firstKey();
         } finally {
            sharedMapRelease();
        }
    }

    private Accumulator getGenerationAccumulator(Session session) throws PersistitException {
        // treespace policy could split the _schema_ tree across volumes and give us multiple accumulators, which would
        // be very bad. Work around that with a fake/constant schema name. It isn't a problem if this somehow got changed
        // across a restart. Really, we want a constant, system-like volume to put this in.
        final String SCHEMA = "pssm";
        Exchange ex = schemaTreeExchange(session, SCHEMA);
        try {
            return ex.getTree().getAccumulator(SCHEMA_GEN_ACCUM_TYPE, SCHEMA_GEN_ACCUM_INDEX);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    private long getGenerationSnapshot(Session session) {
        try {
            return getGenerationAccumulator(session).getSnapshotValue(treeService.getDb().getTransaction());
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private long getNextGeneration(Session session) {
        final int ACCUM_UPDATE_VALUE = 1;   // irrelevant for SEQ types
        try {
            return getGenerationAccumulator(session).update(ACCUM_UPDATE_VALUE, treeService.getDb().getTransaction());
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private void sharedMapClaim() {
        aisMapLock.readLock().lock();
    }

    private void sharedMapRelease() {
        aisMapLock.readLock().unlock();
    }

    private void exclusiveMapClaim() {
        aisMapLock.writeLock().lock();
    }

    private void exclusiveMapRelease() {
        aisMapLock.writeLock().unlock();
    }

    private TableName createTableCommon(Session session, UserTable newTable, boolean isInternal,
                                        Integer version, MemoryTableFactory factory) {
        final TableName newName = newTable.getName();
        checkTableName(session, newName, false, isInternal);
        checkJoinTo(newTable.getParentJoin(), newName, isInternal);
        AISMerge merge = AISMerge.newForAddTable(nameGenerator, getAis(session), newTable);
        merge.merge();
        UserTable mergedTable = merge.getAIS().getUserTable(newName);
        if(factory != null) {
            mergedTable.setMemoryTableFactory(factory);
        }
        if(version != null) {
            mergedTable.setVersion(version);
        }
        AkibanInformationSchema newAIS = merge.getAIS();
        if(factory == null) {
            saveAISChangeWithRowDefs(session, newAIS, Collections.singleton(newName.getSchemaName()));
        } else {
            // Memory only table changed, no reason to re-serialize
            unSavedAISChangeWithRowDefs(session, newAIS);
        }
        try {
            if (mergedTable.getIdentityColumn() != null) {
                mergedTable.getIdentityColumn().getIdentityGenerator().setStartWithAccumulator(treeService);
            }
        } catch (PersistitException ex) {
            LOG.error("Setting sequence starting value for table {} failed", mergedTable.getName().getDescription());
            throw wrapPersistitException(session, ex);
        }
        return getAis(session).getUserTable(newName).getName();
    }

    private static void checkSystemSchema(TableName tableName, boolean shouldBeSystem) {
        String schemaName = tableName.getSchemaName();
        final boolean inSystem = TableName.INFORMATION_SCHEMA.equals(schemaName) ||
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
            boolean inAIS = TableName.INFORMATION_SCHEMA.equals(parentName.getSchemaName());
            if(inAIS && !isInternal) {
                throw new JoinToProtectedTableException(parentName, childName);
            } else if(!inAIS && isInternal) {
                throw new IllegalArgumentException("Internal table join to non-IS table: " + childName);
            }
        }
    }

    private void checkTableName(Session session, TableName tableName, boolean shouldExist, boolean inIS) {
        checkSystemSchema(tableName, inIS);
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

    private void checkAndSerialize(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, String schema) throws PersistitException {
        if(serializationType == SerializationType.NONE) {
            serializationType = DEFAULT_SERIALIZATION;
        }
        if(serializationType == SerializationType.PROTOBUF) {
            saveProtobuf(ex, buffer, newAIS, schema);
        } else {
            throw new IllegalStateException("Cannot serialize as " + serializationType);
        }
    }

    private interface ThrowingCallable<V> {
        public V runAndReturn(Session session) throws PersistitException;
    }

    private static abstract class ThrowingRunnable implements ThrowingCallable<Void> {
        public abstract void run(Session session) throws PersistitException;

        public Void runAndReturn(Session session) throws PersistitException {
            run(session);
            return null;
        }
    }

    private <V> V transactionally(Session session, ThrowingCallable<V> callable) {
        txnService.beginTransaction(session);
        try {
            V ret = callable.runAndReturn(session);
            txnService.commitTransaction(session);
            return ret;
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
            session.close();
        }
    }

    private static final TransactionService.Callback CLEAR_SESSION_KEY_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session) {
            session.remove(SESSION_AIS_KEY);
        }
    };
}
