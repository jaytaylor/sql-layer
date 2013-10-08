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

package com.foundationdb.server.types;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.pvalue.PValueSources;
import com.foundationdb.util.Equality;

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
        //if (tInstance == null)
        //    ArgumentValidation.isNull("value", value);
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
