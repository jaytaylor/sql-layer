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

import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Equality;

public final class TPreptimeValue {

    public void instance(TInstance tInstance) {
        assert mutable : "not mutable";
        this.tInstance = tInstance;
    }

    public TInstance instance() {
        return tInstance;
    }

    public void value(PValueSource value) {
        assert mutable : "not mutable";
        this.value = value;
    }

    public PValueSource value() {
        return value;
    }

    public TPreptimeValue() {
        this.mutable = true;
    }

    public TPreptimeValue(TInstance tInstance) {
        this(tInstance, null);
    }

    public TPreptimeValue(TInstance tInstance, PValueSource value) {
        this.tInstance = tInstance;
        this.value = value;
        this.mutable = false;
        if (value != null)
            tInstance.setNullable(value.isNull());
    }

    @Override
    public String toString() {
        String result = tInstance.toString();
        if (value != null)
            result = result + '=' + value;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TPreptimeValue that = (TPreptimeValue) o;
        
        if (!Equality.areEqual(tInstance, that.tInstance))
            return false;
        if (value == null)
            return that.value == null;
        return that.value != null && PValueSources.areEqual(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = tInstance != null ? tInstance.hashCode() : 0;
        result = 31 * result + (value != null ? PValueSources.hash(value) : 0);
        return result;
    }

    private TInstance tInstance;
    private PValueSource value;
    private boolean mutable; // TODO ugh! should we next this, or create a hierarchy of TPV, MutableTPV?
}
