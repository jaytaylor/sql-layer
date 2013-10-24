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
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.util.layers.DirectorySubspace;

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
 * &lt;root_dir&gt;/
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
 *     during an <code>ALTER</code>. This class provides another second level
 *     sub-directory, {@link #ALTER_PATH_NAME}, which is used for that purpose.
 * </p>
 *
 */
public class FDBNameGenerator implements NameGenerator
{
    private static final String DATA_PATH_NAME = "data";
    private static final String ALTER_PATH_NAME = "dataAlter";
    private static final String TABLE_PATH_NAME = "table";
    private static final String SEQUENCE_PATH_NAME = "sequence";


    private final Transaction txn;
    private final DirectorySubspace directory;
    private final String pathPrefix;
    private final NameGenerator wrapped;


    private FDBNameGenerator(Transaction txn, DirectorySubspace dir, String pathPrefix, NameGenerator wrapped) {
        this.txn = txn;
        this.directory = dir;
        this.pathPrefix = pathPrefix;
        this.wrapped = wrapped;
    }

    public static FDBNameGenerator createForDataPath(Transaction txn, DirectorySubspace dir, NameGenerator wrapped) {
        return new FDBNameGenerator(txn, dir, DATA_PATH_NAME, wrapped);
    }

    public static FDBNameGenerator createForAlterPath(Transaction txn, DirectorySubspace dir, NameGenerator wrapped) {
        return new FDBNameGenerator(txn, dir, ALTER_PATH_NAME, wrapped);
    }

    @Override
    public NameGenerator unwrap() {
        return this;
    }

    //
    // DATA_PATH_NAME helpers
    //

    public static Tuple dataPath(TableName tableName) {
        return dataPath(tableName.getSchemaName(), tableName.getTableName());
    }

    public static Tuple dataPath(String schemaName, String tableName) {
        return makeTablePath(DATA_PATH_NAME, schemaName, tableName);
    }

    public static Tuple dataPath(Index index) {
        return makeIndexPath(DATA_PATH_NAME, index);
    }

    public static Tuple dataPath(Sequence sequence) {
        return makeSequencePath(DATA_PATH_NAME, sequence);
    }


    //
    // ALTER_PATH_NAME helpers
    //

    public static Tuple alterPath(TableName tableName) {
        return alterPath(tableName.getSchemaName(), tableName.getTableName());
    }

    public static Tuple alterPath(String schemaName, String tableName) {
        return makeTablePath(ALTER_PATH_NAME, schemaName, tableName);
    }

    public static Tuple alterPath(Index index) {
        return makeIndexPath(ALTER_PATH_NAME, index);
    }

    public static Tuple alterPath(Sequence sequence) {
        return makeSequencePath(ALTER_PATH_NAME, sequence);
    }


    //
    // Directory based generation
    //

    public byte[] generateIndexPrefixBytes(Index index) {
        return generate(makeIndexPath(pathPrefix, index));
    }

    public byte[] generateGroupPrefixBytes(String schemaName, String groupName) {
        return generate(makeTablePath(pathPrefix, schemaName, groupName));
    }

    public byte[] generateSequencePrefixBytes(Sequence sequence) {
        return generate(makeSequencePath(pathPrefix, sequence));
    }


    //
    // Tree name based generation: only used for full text indexes.
    //

    @Override
    public String generateIndexTreeName(Index index) {
        return wrapped.generateIndexTreeName(index);
    }

    @Override
    public String generateGroupTreeName(String schemaName, String groupName) {
        return wrapped.generateGroupTreeName(schemaName, groupName);
    }

    @Override
    public String generateSequenceTreeName(Sequence sequence) {
        return wrapped.generateSequenceTreeName(sequence);
    }

    @Override
    public void generatedTreeName(String treeName) {
        wrapped.generatedTreeName(treeName);
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
    public TableName generateIdentitySequenceName(TableName table) {
        return wrapped.generateIdentitySequenceName(table);
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
    public void mergeAIS(AkibanInformationSchema ais) {
        wrapped.mergeAIS(ais);
    }

    @Override
    public void removeTableID(int tableID) {
        wrapped.removeTableID(tableID);
    }

    @Override
    public void removeTreeName(String treeName) {
        wrapped.removeTreeName(treeName);
    }

    @Override
    public Set<String> getTreeNames() {
        return wrapped.getTreeNames();
    }


    //
    // Internal
    //

    private byte[] generate(Tuple path) {
        // Directory should always hand out unique prefixes.
        // So use createOrOpen() and do not pass to wrapped for unique check as AISValidation confirms
        DirectorySubspace indexDir = directory.createOrOpen(txn, path);
        return indexDir.pack();
    }


    //
    // Helpers
    //

    public static Tuple makeTablePath(String pathPrefix, String schemaName, String tableName) {
        return Tuple.from(pathPrefix, TABLE_PATH_NAME, schemaName, tableName);
    }

    public static Tuple makeIndexPath(String pathPrefix, Index index) {
        IndexName name = index.getIndexName();
        return Tuple.from(pathPrefix, TABLE_PATH_NAME, name.getSchemaName(), name.getTableName(), name.getName());
    }

    public static Tuple makeSequencePath(String pathPrefix, Sequence sequence) {
        TableName name = sequence.getSequenceName();
        return Tuple.from(pathPrefix, SEQUENCE_PATH_NAME, name.getSchemaName(), name.getTableName());
    }
}
