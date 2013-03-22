
package com.akiban.server.types3;

import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.TString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.ArgumentValidation;
import com.akiban.util.Equality;

public final class TPreptimeValue {

    public void instance(TInstance tInstance) {
        assert mutable : "not mutable";
        this.tInstance = tInstance;
    }

    public boolean isNullable() {
        return tInstance == null || tInstance.nullability();
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
        if (tInstance == null)
            ArgumentValidation.isNull("value", value);
    }

    @Override
    public String toString() {
        if (tInstance == null)
            return "<unknown>";
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
        return that.value != null && PValueSources.areEqual(value, that.value, tInstance);
    }

    @Override
    public int hashCode() {
        int result = tInstance != null ? tInstance.hashCode() : 0;
        AkCollator collator;
        if (tInstance != null && tInstance.typeClass() instanceof TString) {
            int charsetId = tInstance.attribute(StringAttribute.CHARSET);
            collator = AkCollatorFactory.getAkCollator(charsetId);
        }
        else {
            collator = null;
        }
        result = 31 * result + (value != null ? PValueSources.hash(value, collator) : 0);
        return result;
    }

    private TInstance tInstance;
    private PValueSource value;
    private boolean mutable; // TODO ugh! should we next this, or create a hierarchy of TPV, MutableTPV?
}
