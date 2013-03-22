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

package com.akiban.server.geophile;

// Relationship of a Region to a SpatialObject

public enum RegionComparison
{
    // A single region comparison
    OUTSIDE(0x0), // region outside spatial object
    OVERLAP(0x1),  // region overlaps spatial object
    INSIDE(0x2),  // region inside spatial object
    
    // Combined left and right comparisons
    OUTSIDE_OUTSIDE(0xf00),
    OUTSIDE_OVERLAP(0xf01),
    OUTSIDE_INSIDE(0xf02),
    OVERLAP_OUTSIDE(0xf10),
    OVERLAP_OVERLAP(0xf11),
    OVERLAP_INSIDE(0xf12),
    INSIDE_OUTSIDE(0xf20),
    INSIDE_OVERLAP(0xf21),
    INSIDE_INSIDE(0xf22);
    
    RegionComparison concat(RegionComparison that)
    {
        switch (0xf00 | (this.code << 4) | that.code) {
            case 0xf00: return OUTSIDE_OUTSIDE;
            case 0xf01: return OUTSIDE_OVERLAP;
            case 0xf02: return OUTSIDE_INSIDE;
            case 0xf10: return OVERLAP_OUTSIDE;
            case 0xf11: return OVERLAP_OVERLAP;
            case 0xf12: return OVERLAP_INSIDE;
            case 0xf20: return INSIDE_OUTSIDE;
            case 0xf21: return INSIDE_OVERLAP;
            case 0xf22: return INSIDE_INSIDE;
            default:
                assert false;
                return null;
        }
    }

    private RegionComparison(int code)
    {
        this.code = code;
    }

    private final int code;
}
