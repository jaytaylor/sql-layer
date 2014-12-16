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
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

import java.util.Objects;

public final class TPreptimeValue {

    public void type(TInstance type) {
        assert mutable : "not mutable";
        this.type = type;
    }

    public boolean isNullable() {
        return type == null || type.nullability();
    }

    public TInstance type() {
        return type;
    }

    public void value(ValueSource value) {
        assert mutable : "not mutable";
        this.value = value;
    }

    public ValueSource value() {
        return value;
    }

    public TPreptimeValue() {
        this.mutable = true;
    }

    public TPreptimeValue(TInstance type) {
        this(type, null);
    }

    public TPreptimeValue(ValueSource value) {
        this(value.getType(), value);
    }

    public TPreptimeValue(TInstance type, ValueSource value) {
        this.type = type;
        this.value = value;
        this.mutable = false;
        //if (type == null)
        //    ArgumentValidation.isNull("value", value);
    }

    @Override
    public String toString() {
        if (type == null)
            return "<unknown>";
        String result = type.toString();
        if (value != null)
            result = result + '=' + value;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TPreptimeValue that = (TPreptimeValue) o;
        
        if (!Objects.deepEquals(type, that.type))
            return false;
        if (value == null)
            return that.value == null;
        return that.value != null && TClass.areEqual(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        AkCollator collator;
        if (type != null && type.typeClass() instanceof TString) {
            collator = AkCollatorFactory.getAkCollator(type.attribute(StringAttribute.COLLATION));
        }
        else {
            collator = null;
        }
        result = 31 * result + (value != null ? ValueSources.hash(value, collator) : 0);
        return result;
    }

    private TInstance type;
    private ValueSource value;
    private boolean mutable; // TODO ugh! should we next this, or create a hierarchy of TPV, MutableTPV?
}
