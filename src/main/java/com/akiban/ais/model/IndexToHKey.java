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

/**
 * IndexToHKey is an interface useful in constructing HKey values from an index row.
 * There are two types of entries, ordinal values and index fields. An ordinal identifies
 * a user table. Non-ordinal entries are the positions within the index row and the
 * table where the HKey values can be found.
 */
public class IndexToHKey {
    public IndexToHKey(int[] ordinals,
                       int[] indexRowPositions,
                       int[] fieldPositions) {
        if(ordinals.length != indexRowPositions.length || ordinals.length != fieldPositions.length) {
            throw new IllegalArgumentException("All arrays must be of equal length: " +
                                               ordinals.length + ", " +
                                               indexRowPositions.length + ", " +
                                               fieldPositions.length);
        }
        this.ordinals = ordinals;
        this.indexRowPositions = indexRowPositions;
        this.fieldPositions = fieldPositions;
    }

    public boolean isOrdinal(int index) {
        return ordinals[index] >= 0;
    }

    public int getOrdinal(int index) {
        return ordinals[index];
    }

    public int getIndexRowPosition(int index) {
        return indexRowPositions[index];
    }

    public int getFieldPosition(int index) {
        return fieldPositions[index];
    }

    public int getLength() {
        return ordinals.length;
    }

    /** If set, value >= 0, the ith field of the hkey is this ordinal **/
    private final int[] ordinals;
    /** If set, value >= 0, the ith field of the hkey is at this position in the index row **/
    private final int[] indexRowPositions;
    /** If set, value >= 0, the ith field of the hkey is at this field in the data row **/
    private final int[] fieldPositions;
}
