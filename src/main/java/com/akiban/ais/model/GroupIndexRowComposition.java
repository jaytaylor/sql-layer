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

package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupIndexRowComposition
{
    @Override
    public String toString()
    {
        copyToArrays();
        StringBuilder buffer = new StringBuilder();
        buffer.append("positionInFlattenedRow: ");
        buffer.append(Arrays.toString(positionInFlattenedRow));
        return buffer.toString();
    }

    public void addField(int positionInFlattenedRow)
    {
        positionInFlattenedRowList.add(positionInFlattenedRow);
        equivalentHKeyIndexPositionsLists.add(null);
    }

    public void addField(int positionInFlattenedRow, List<Integer> equivalentHKeyColumnPositions)
    {
        positionInFlattenedRowList.add(positionInFlattenedRow);
        equivalentHKeyIndexPositionsLists.add(equivalentHKeyColumnPositions);
    }

    public int positionInFlattenedRow(int indexPosition)
    {
        return positionInFlattenedRow[indexPosition];
    }

    public int size()
    {
        return positionInFlattenedRow.length;
    }

    public int[] equivalentHKeyIndexPositions(int indexPos)
    {
        return equivalentHKeyIndexPositions[indexPos];
    }

    public void close()
    {
        copyToArrays();
        positionInFlattenedRowList = null;
        equivalentHKeyIndexPositionsLists = null;
    }

    private void copyToArrays()
    {
        if (positionInFlattenedRowList != null) {
            int n = positionInFlattenedRowList.size();
            assert equivalentHKeyIndexPositionsLists.size() == n;
            positionInFlattenedRow = new int[n];
            equivalentHKeyIndexPositions = new int[n][];
            for (int i = 0; i < n; i++) {
                positionInFlattenedRow[i] = positionInFlattenedRowList.get(i);
                List<Integer> equivalentHKeyIndexPositionsList = equivalentHKeyIndexPositionsLists.get(i);
                if (equivalentHKeyIndexPositionsList != null) {
                    equivalentHKeyIndexPositions[i] = new int[equivalentHKeyIndexPositionsList.size()];
                    for (int j = 0; j < equivalentHKeyIndexPositionsList.size(); j++) {
                        equivalentHKeyIndexPositions[i][j] = equivalentHKeyIndexPositionsList.get(j);
                    }
                }
            }
        }
    }

    // For use by this class



    // Object state

    // While building
    private List<Integer> positionInFlattenedRowList = new ArrayList<Integer>();
    private List<List<Integer>> equivalentHKeyIndexPositionsLists = new ArrayList<List<Integer>>();
    // Once built
    private int[] positionInFlattenedRow;
    private int[][] equivalentHKeyIndexPositions;
}
