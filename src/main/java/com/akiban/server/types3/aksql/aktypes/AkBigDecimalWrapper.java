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

package com.akiban.server.types3.aksql.aktypes;

import com.akiban.server.types3.common.BigDecimalWrapper;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class AkBigDecimalWrapper implements BigDecimalWrapper {
    
    @Override
    public void reset() {
        value = BigDecimal.ZERO;
    }
            
    @Override
    public BigDecimalWrapper add(BigDecimalWrapper other) {
        AkBigDecimalWrapper o = (AkBigDecimalWrapper) other;
        value = value.add(o.value);
        return this;
    }

    @Override
    public BigDecimalWrapper subtract(BigDecimalWrapper other) {
        AkBigDecimalWrapper o = (AkBigDecimalWrapper) other;
        value = value.subtract(o.value);
        return this;
    }

    @Override
    public BigDecimalWrapper multiply(BigDecimalWrapper other) {
        AkBigDecimalWrapper o = (AkBigDecimalWrapper) other;
        value = value.multiply(o.value);
        return this;
    }

    @Override
    public BigDecimalWrapper divide(BigDecimalWrapper other) {
        AkBigDecimalWrapper o = (AkBigDecimalWrapper) other;
        value = value.divide(o.value);
        return this;
    }

    @Override
    public BigDecimalWrapper divide(BigDecimalWrapper augend, int scale)
    {
        value = value.divide(((AkBigDecimalWrapper)augend).value,
                scale,
                RoundingMode.HALF_UP);
        return this;
    }

    @Override
    public int getSign() {
        return value.signum();
    }
        
    private BigDecimal value;
}
