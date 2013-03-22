
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
