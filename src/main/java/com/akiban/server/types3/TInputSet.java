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

package com.akiban.server.types3;

import java.util.BitSet;

public final class TInputSet {

    public boolean isPicking() {
        return isPicking;
    }

    public TClass targetType() {
        return targetType;
    }

    public int positionsLength() {
        return covering.length();
    }

    public boolean covers(int index) {
        return covering.get(index);
    }

    public boolean coversRemaining() {
        return coversRemaining;
    }

    public int firstPosition() {
        return covering.nextSetBit(0);
    }

    public int nextPosition(int from) {
        return covering.nextSetBit(from);
    }

    public TInputSet(TClass targetType, BitSet covering, boolean coversRemaining, boolean isPicking) {
        this.targetType = targetType;
        this.covering = covering.get(0, covering.length());
        this.coversRemaining = coversRemaining;
        this.isPicking = isPicking;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean coversAny = ! covering.isEmpty();
        if (coversAny) {
            sb.append("POS(");
            for (int i = covering.nextSetBit(0); i >= 0; i = covering.nextSetBit(i+1)) {
                sb.append(i).append(", ");
            }
            sb.setLength(sb.length() - 2); // trim trailing ", "
            sb.append(')');
        }
        if (coversRemaining) {
            if (coversAny)
                sb.append(", ");
            sb.append("REMAINING");
        }
        if (sb.length() == 0)
            sb.append("<none>"); // malformed input set, but still want a decent toString
        sb.append(" <- ").append(targetType);
        return sb.toString();
    }

    private final TClass targetType;
    private final BitSet covering;
    private final boolean coversRemaining;
    private final boolean isPicking;
}
