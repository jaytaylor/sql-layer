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
