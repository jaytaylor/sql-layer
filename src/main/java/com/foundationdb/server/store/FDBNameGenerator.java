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

import com.foundationdb.Transaction;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * FDB-aware NameGenerator that generates tree names based off of a
 * {@link DirectorySubspace}.
 *
 * <p>
 *     NameGenerator has distinct methods that generate trees for groups,
 *     indexes and sequences. Each of these have their own, distinct scopes
 *     of enforced uniqueness (schema, table and schema, respectively).
 * </p>
 *
 * <p>
 * The combination of these will produce a set of directories under
 * {@link #DATA_PATH_NAME} for COI created in the test schema like:
 *
 * <pre>
 * root_dir/
 *   data/
 *     sequence/
 *       test/
 *         customers/
 *           _c_pk_seq/
 *         orders/
 *           _o_pk_seq/
 *         items/
 *           _i_pk_seq/
 *     table/
 *       test/
 *         customers/
 *           PRIMARY/
 *           idx_name/
 *           g_idx_odate_cname/
 *         orders/
 *           PRIMARY
 *           idx_date/
 *         items/
 *           PRIMARY/
 *           idx_sku/
 * </pre>
 * </p>
 *
 * <p>
 *     The above example shows a directory layout after a schema has been
 *     created. Trees names are also required to be generated, and distinct,
 *     during an online change. This class provides another second level
 *     sub-directory, {@link #ONLINE_PATH_NAME}, which is used for that purpose.
 * </p>
 *
 */
public class FDBNameGenerator implements NameGenerator
{
    static final String DATA_PATH_NAME = "data";
    static final String ONLINE_PATH_NAME = "dataOnline";
    static final String TABLE_PATH_NAME = "table";
    static final String SEQUENCE_PATH_NAME = "sequence";


    private final TransactionState txn;
    private final DirectorySubspace directory;
    private final String pathPrefix;
    private final NameGenerator wrapped;


    private FDBNameGenerator(TransactionState txn, DirectorySubspace dir, String pathPrefix, NameGenerator wrapped) {
        this.txn = txn;
        this.directory = dir;
        this.pathPrefix = pathPrefix;
        this.wrapped = wrapped;
    }

    public static FDBNameGenerator createForDataPath(TransactionState txn, DirectorySubspace dir, NameGenerator wrapped) {
        return new FDBNameGenerator(txn, dir, DATA_PATH_NAME, wrapped);
    }

    public static FDBNameGenerator createForOnlinePath(TransactionState txn, DirectorySubspace dir, NameGenerator wrapped) {
        return new FDBNameGenerator(txn, dir, ONLINE_PATH_NAME, wrapped);
    }

    //
    // DATA_PATH_NAME helpers
    //

    public static List<String> dataPath(TableName tableName) {
        return dataPath(tableName.getSchemaName(), tableName.getTableName());
    }

    public static List<String> dataPath(String schemaName, String tableName) {
        return makeTablePath(DATA_PATH_NAME, schemaName, tableName);
    }

    public static List<String> dataPath(Index index) {
        return makeIndexPath(DATA_PATH_NAME, index);
    }

    public static List<String> dataPath(Sequence sequence) {
        TableName seqName = sequence.getSequenceName();
        return dataPathSequence(seqName.getSchemaName(), seqName.getTableName());
    }

    public static List<String> dataPathSequence(String schemaName, String sequenceName) {
        return makeSequencePath(DATA_PATH_NAME, schemaName, sequenceName);
    }


    //
    // ONLINE_PATH_NAME helpers
    //

    public static List<String> onlinePath(TableName tableName) {
        return onlinePath(tableName.getSchemaName(), tableName.getTableName());
    }

    public static List<String> onlinePath(String schemaName, String tableName) {
        return makeTablePath(ONLINE_PATH_NAME, schemaName, tableName);
    }

    public static List<String> onlinePath(Index index) {
        return makeIndexPath(ONLINE_PATH_NAME, index);
    }

    public static List<String> onlinePath(Sequence sequence) {
        TableName seqName = sequence.getSequenceName();
        return onlinePathSequence(seqName.getSchemaName(), seqName.getTableName());
    }

    public static List<String> onlinePathSequence(String schemaName, String sequenceName) {
        return makeSequencePath(ONLINE_PATH_NAME, schemaName, sequenceName);
    }


    //
    // Directory based generation
    //

    public synchronized byte[] generateIndexPrefixBytes(Index index) {
        return generate(makeIndexPath(pathPrefix, index));
    }

    public synchronized byte[] generateGroupPrefixBytes(String schemaName, String groupName) {
        return generate(makeTablePath(pathPrefix, schemaName, groupName));
    }

    public synchronized byte[] generateSequencePrefixBytes(Sequence sequence) {
        TableName seqName = sequence.getSequenceName();
        return generate(makeSequencePath(pathPrefix, seqName.getSchemaName(), seqName.getTableName()));
    }


    //
    // Trivially wrapped
    //

    @Override
    public int generateTableID(TableName name) {
        return wrapped.generateTableID(name);
    }

    @Override
    public int generateIndexID(int rootTableID) {
        return wrapped.generateIndexID(rootTableID);
    }

    @Override
    public TableName generateIdentitySequenceName(AkibanInformationSchema ais, TableName table, String column) {
        return wrapped.generateIdentitySequenceName(ais, table, column);
    }

    @Override
    public String generateJoinName(TableName parentTable, TableName childTable, String[] pkColNames, String[] fkColNames) {
        return wrapped.generateJoinName(parentTable, childTable, pkColNames, fkColNames);
    }

    @Override
    public String generateJoinName(TableName parentTable, TableName childTable, List<JoinColumn> joinIndex) {
        return wrapped.generateJoinName(parentTable, childTable, joinIndex);
    }

    @Override
    public String generateJoinName(TableName parentTable,
                                   TableName childTable,
                                   List<String> pkColNames,
                                   List<String> fkColNames) {
        return wrapped.generateJoinName(parentTable, childTable, pkColNames, fkColNames);
    }

    @Override
    public String generateFullTextIndexPath(FullTextIndex index) {
        return wrapped.generateFullTextIndexPath(index);
    }

    @Override
    public void mergeAIS(AkibanInformationSchema ais) {
        wrapped.mergeAIS(ais);
    }

    @Override
    public void removeTableID(int tableID) {
        wrapped.removeTableID(tableID);
    }

    @Override
    public Set<String> getStorageNames() {
        return wrapped.getStorageNames();
    }

    @Override
    public TableName generateFKConstraintName(String schemaName, String tableName) {
        return wrapped.generateFKConstraintName(schemaName, tableName);
    }

    @Override
    public TableName generatePKConstraintName(String schemaName, String tableName) {
        return wrapped.generatePKConstraintName(schemaName, tableName);
    }

    @Override
    public TableName generateUniqueConstraintName(String schemaName, String tableName) {
        return wrapped.generateUniqueConstraintName(schemaName, tableName);
    }

    //
    // Internal
    //

    private byte[] generate(List<String> path) {
        // Directory should always hand out unique prefixes.
        // So use createOrOpen() and do not pass to wrapped for unique check as AISValidation confirms
        try {
            Transaction txn = this.txn.getTransaction();
            DirectorySubspace indexDir = directory.createOrOpen(txn, path).get();
            return indexDir.pack();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(this.txn.session, e);
        }
    }


    //
    // Helpers
    //

    public static List<String> makeTablePath(String pathPrefix, String schemaName, String tableName) {
        return Arrays.asList(pathPrefix, TABLE_PATH_NAME, schemaName, tableName);
    }

    public static List<String> makeIndexPath(String pathPrefix, Index index) {
        IndexName name = index.getIndexName();
        return Arrays.asList(pathPrefix, TABLE_PATH_NAME, name.getSchemaName(), name.getTableName(), name.getName());
    }

    public static List<String> makeSequencePath(String pathPrefix, String schemaName, String sequenceName) {
        return Arrays.asList(pathPrefix, SEQUENCE_PATH_NAME, schemaName, sequenceName);
    }
}
