/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.store;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.TableChange;
import com.akiban.server.error.NoSuchColumnException;
import com.akiban.server.error.NoSuchSchemaException;
import com.akiban.server.error.NoSuchSequenceException;
import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.dxl.DXLFunctionsHook;
import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.sql.server.ServerCallContextStack;
import com.akiban.sql.server.ServerQueryContext;
import com.akiban.sql.server.ServerSession;
import com.persistit.exception.PersistitException;

import java.util.Arrays;

import static com.akiban.server.service.transaction.TransactionService.CloseableTransaction;

/**
 * <p>
 *     Added in support of bug1167045. These routines allow for a manual fix of existing databases with sequences.
 * </p>
 * <p>
 *     Prior to 1.6.1, SEQUENCEs were backed by a SUM accumulator and used in a non-safe way. Particularly, rollbacks
 *     would cause value reuse upon restart. Post bug fix, they are backed by a SEQ under a different algorithm.
 * </p>
 * <p>
 *     seq_tree_rest allows for an existing sequence to be cleared and pegged at a new value.
 * </p>
 * <p>
 *     seq_identity_default_to_always allows for an IDENTITY column to be set to ALWAYS instead of DEFAULT, which
 *     is the new default behavior for SERIAL columns.
 * </p>
 */
public class SequenceFixUpRoutines {
    private static final DXLFunctionsHook.DXLFunction LOCK_FUNC = DXLFunctionsHook.DXLFunction.UNSPECIFIED_DDL_WRITE;

    private SequenceFixUpRoutines() {
    }

    public static void seq_tree_reset(String seqSchema, String seqName, long newValue) throws InterruptedException {
        if(!DXLReadWriteLockHook.only().isDDLLockEnabled()) {
            throw new IllegalStateException("Unsafe to use with global lock disabled");
        }

        final ServerQueryContext context = ServerCallContextStack.current().getContext();
        final ServerSession server = context.getServer();
        final Session session = server.getSession();
        if(!server.getSecurityService().isAccessible(session, seqSchema)) {
            throw new NoSuchSchemaException(seqSchema);
        }

        // Global lock
        DXLReadWriteLockHook.only().lock(session, LOCK_FUNC, -1);
        try {
            // Find sequence
            Sequence seq = server.getDXL().ddlFunctions().getAIS(session).getSequence(new TableName(seqSchema, seqName));
            if(seq == null) {
                throw new NoSuchSequenceException(seqSchema, seqName);
            }
            TreeService treeService = server.getTreeService();
            // Drop tree
            treeService.getExchange(session, seq).removeTree();
            // Create tree
            treeService.populateTreeCache(seq);
            try(CloseableTransaction txn = server.getTransactionService().beginCloseableTransaction(session)) {
                for(int i = 0; i < newValue; ++i) {
                    seq.nextValue(treeService);
                }
                txn.commit();
            }
        } catch(PersistitException e) {
            throw new PersistitAdapterException(e);
        } finally {
            // unlock
            DXLReadWriteLockHook.only().unlock(server.getSession(), LOCK_FUNC);
        }
    }

    public static void seq_identity_default_to_always(String schema, String table, String column) {
        final ServerQueryContext context = ServerCallContextStack.current().getContext();
        final ServerSession server = context.getServer();
        final Session session = server.getSession();
        if(!server.getSecurityService().isAccessible(session, schema)) {
            throw new NoSuchSchemaException(schema);
        }

        TableName tableName = new TableName(schema, table);
        AkibanInformationSchema ais = server.getAIS();
        UserTable curTable = ais.getUserTable(tableName);
        if(curTable == null) {
            throw new NoSuchTableException(tableName);
        }
        Column curColumn = curTable.getColumn(column);
        if(curColumn == null) {
            throw new NoSuchColumnException(column);
        }
        if(curColumn.getDefaultIdentity() == null) {
            throw new IllegalArgumentException("Column " + column + " is not an IDENTITY");
        }

        AkibanInformationSchema copy = AISCloner.clone(ais, new ProtobufWriter.SingleSchemaSelector(schema));
        UserTable newTable = copy.getUserTable(schema, table);
        newTable.getColumn(column).setDefaultIdentity(false);
        server.getDXL().ddlFunctions().alterTable(
                session, tableName, newTable,
                Arrays.asList(TableChange.createModify(column, column)),
                Arrays.<TableChange>asList(),
                context
        );
    }
}
