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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.rowdata.RowData;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AncestorDiscoveryIterator implements Iterator<RowData>
{
    // Iterator interface

    @Override
    public boolean hasNext()
    {
        return current != null;
    }

    @Override
    public RowData next()
    {
        if (current != null) {
            // Find the hkey segment at which the current row differs from the previous. This is needed to
            // fill in missing ancestors of orphan rows later. The input iterator provides rows in index order
            // for the predicate, with each row of the predicate type followed by hkey ordered descendents.
            // The divergence position is only relevant for the hkey-ordered runs of the input. So for rows
            // shallower than the predicate table, set divergence position to 0.
            if (current.differsFromPredecessorAtKeySegment() == -1) {
                int divergencePosition;
                if (previous == null) {
                    divergencePosition = 0;
                } else {
                    UserTable table = ais.getUserTable(current.getRowDefId());
                    assert table != null
                            : "null table " + current.getRowDefId() + " from previous " + previous.getRowDefId()
                            + ". tables map: " + describeTablesMap(ais);
                    if (table.getDepth() < predicateTableDepth) {
                        if (table.isRoot()) {
                            divergencePosition = 0;
                        } else {
                            HKey hKey = table.parentTable().hKey();
                            divergencePosition = hKey.nColumns() + hKey.segments().size();
                        }
                    } else {
                        divergencePosition = current.hKey().firstUniqueSegmentDepth(previous.hKey());
                    }
                }
                current.differsFromPredecessorAtKeySegment(divergencePosition);
            } // else: differsFromPredecessorAtKeySegment has already been set, presumably as part of a test.
            previous = current;
            current = input.hasNext() ? input.next() : null;
        } else {
            previous = null;
        }
/*
        if (previous == null) {
            System.out.println("ADI: null");
        } else {
            System.out.println(String.format("ADI: %s: %s", previous.differsFromPredecessorAtKeySegment(), previous));
        }
*/
        return previous;
    }

    private static String describeTablesMap(AkibanInformationSchema ais) {
        // please please, Java, can't we have list comprehension and lambdas? :-(
        Map<Integer,TableName> map = new HashMap<Integer, TableName>();
        for (Map.Entry<TableName,UserTable> entry : ais.getUserTables().entrySet()) {
            map.put(entry.getValue().getTableId(), entry.getKey());
        }
        return map.toString();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // AncestorDiscoveryIterator interface

    AncestorDiscoveryIterator(UserTable predicateTable, boolean hKeyOrdered, Iterator<RowData> input)
    {
        this.ais = predicateTable.getAIS();
        this.predicateTableDepth = predicateTable.getDepth();
        this.hKeyOrdered = hKeyOrdered;
        this.input = input;
        if (input.hasNext()) {
            this.current = input.next();
        }
    }

    // Object state

    private final AkibanInformationSchema ais;
    private final int predicateTableDepth;
    private final boolean hKeyOrdered;
    private final Iterator<RowData> input;
    private RowData previous;
    private RowData current;
}
