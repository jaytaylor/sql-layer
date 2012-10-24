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

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.TableIndex;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.PersistitStore;

public class OneTableRowCollector extends OperatorBasedRowCollector
{
    OneTableRowCollector(ConfigurationService config,
                         Session session,
                         PersistitStore store,
                         RowDef rowDef,
                         int indexId,
                         int scanFlags,
                         RowData start,
                         ColumnSelector startColumns,
                         RowData end,
                         ColumnSelector endColumns)
    {
        super(store, session, config);
        // rootmostQueryTable
        queryRootTable = rowDef.userTable();
        queryRootType = schema.userTableRowType(queryRootTable);
        requiredUserTables.add(queryRootTable);
        // predicateIndex and predicateType
        predicateIndex = null;
        if (indexId > 0) {
            for (TableIndex userTableIndex : queryRootTable.getIndexesIncludingInternal()) {
                if (userTableIndex.getIndexId() == indexId) {
                    predicateIndex = userTableIndex;
                }
            }
            assert predicateIndex != null : String.format("rowDef: %s, indexId: %s", rowDef, indexId);
        }
        predicateType = queryRootType;
        if (predicateIndex != null) {
            // Index bounds
            IndexRowType indexRowType = schema.indexRowType(predicateIndex).physicalRowType();
            if (start == null && end == null) {
                indexKeyRange = IndexKeyRange.unbounded(indexRowType);
            } else {
                ColumnSelector indexStartSelector = indexSelectorFromTableSelector(predicateIndex, startColumns);
                ColumnSelector indexEndSelector = indexSelectorFromTableSelector(predicateIndex, endColumns);
                IndexBound lo = null;
                if (start != null) {
                    assert start.getRowDefId() == queryRootTable.getTableId();
                    NewRow loRow = new LegacyRowWrapper(session, start, store);
                    lo = new IndexBound(new NewRowBackedIndexRow(queryRootType, loRow, predicateIndex), indexStartSelector);
                }
                IndexBound hi = null;
                if (end != null) {
                    assert end.getRowDefId() == queryRootTable.getTableId();
                    NewRow hiRow = new LegacyRowWrapper(session, end, store);
                    hi = new IndexBound(new NewRowBackedIndexRow(queryRootType, hiRow, predicateIndex), indexEndSelector);
                }
                boolean loInclusive = start != null && (scanFlags & (SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_START_EXCLUSIVE)) == 0;
                boolean hiInclusive = end != null && (scanFlags & (SCAN_FLAGS_END_AT_EDGE | SCAN_FLAGS_END_EXCLUSIVE)) == 0;
                indexKeyRange =
                    lo == null
                    ? IndexKeyRange.endingAt(indexRowType, hi, hiInclusive) :
                    hi == null
                    ? IndexKeyRange.startingAt(indexRowType, lo, loInclusive)
                    : IndexKeyRange.startingAtAndEndingAt(indexRowType,
                                                          lo,
                                                          loInclusive,
                                                          hi,
                                                          hiInclusive);
            }
        }
    }
}
