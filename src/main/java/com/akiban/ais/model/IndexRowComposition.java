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

import java.util.Arrays;

/**
 * IndexRowComposition presents an interface for mapping row and hkey fields
 * to the fields of an index. The leading index fields are exactly the fields
 * identified in the Index (i.e. the declared index columns). The remaining
 * fields are whatever is necessary to ensure that all of the hkey is represented.
 */
public class IndexRowComposition {
    public IndexRowComposition(int[] fieldPositions, int[] hkeyPositions) {
        if(fieldPositions.length != hkeyPositions.length) {
            throw new IllegalArgumentException("Both arrays must be of equal length: " +
                                               fieldPositions.length + ", " +
                                               hkeyPositions.length);
        }
        this.fieldPositions = fieldPositions;
        this.hkeyPositions = hkeyPositions;
    }

    public boolean isInRowData(int indexPos) {
        return fieldPositions[indexPos] >= 0;
    }

    public boolean isInHKey(int indexPos) {
        return hkeyPositions[indexPos] >= 0;
    }

    public int getFieldPosition(int indexPos) {
        return fieldPositions[indexPos];
    }

    public int getHKeyPosition(int indexPos) {
        return hkeyPositions[indexPos];
    }

    public int getLength() {
        return fieldPositions.length;
    }

    @Override
    public String toString() {
        return "fieldPos: " + Arrays.toString(fieldPositions) +
               " hkeyPos: " + Arrays.toString(hkeyPositions);
    }

    /** If set, value >= 0, is the field position for index position i **/
    private final int[] fieldPositions;
    /** If set, value >= 0, is the hkey position for index position i **/
    private final int[] hkeyPositions;
}
