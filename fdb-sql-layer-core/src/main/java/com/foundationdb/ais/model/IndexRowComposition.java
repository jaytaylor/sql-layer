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

package com.foundationdb.ais.model;

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
