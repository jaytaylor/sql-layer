/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
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
    public IndexRowComposition(int[] depths, int[] fieldPositions, int[] hkeyPositions) {
        if(depths.length != fieldPositions.length || depths.length != hkeyPositions.length) {
            throw new IllegalArgumentException(String.format("All indexes must be of equal length: %d,%d,%d",
                                               depths.length, fieldPositions.length, hkeyPositions.length));
        }
        this.depths = depths;
        this.fieldPositions = fieldPositions;
        this.hkeyPositions = hkeyPositions;
    }

    public boolean isInRowData(int indexPos) {
        return fieldPositions[indexPos] >= 0;
    }

    public boolean isInHKey(int indexPos) {
        return hkeyPositions[indexPos] >= 0;
    }

    public int getDepth(int indexPos) {
        return depths[indexPos];
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
        return String.format("depths:%s fieldPos:%s hkeyPos:%s", Arrays.toString(depths),
                             Arrays.toString(fieldPositions), Arrays.toString(hkeyPositions));
    }

    /** If set, value >= 0, is the depth of the associated table for index position i **/
    private final int[] depths;
    /** If set, value >= 0, is the field position for index position i **/
    private final int[] fieldPositions;
    /** If set, value >= 0, is the hkey position for index position i **/
    private final int[] hkeyPositions;
}
