/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.foundationdb.server.types.service.ReflectiveInstanceFinder;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSources;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public abstract class TypeComparisonTestBase
{
    protected static class TypeInfo {
        public final TClass type;
        public final Object min;
        public final Object zero;
        public final Object max;

        public TypeInfo(TClass type, Object min, Object zero, Object max) {
            this.type = type;
            this.min = min;
            this.zero = zero;
            this.max = max;
        }
    }

    protected static TypeInfo typeInfo(TClass type, Object min, Object max) {
        return typeInfo(type, min, null, max);
    }

    protected static TypeInfo typeInfo(TClass type, Object min, Object zero, Object max) {
        return new TypeInfo(type, min, zero, max);
    }

    public static Collection<Object[]> makeParams(TBundleID bundle, Collection<TypeInfo> typeInfos, Collection<TClass> ignore) throws Exception {
        ReflectiveInstanceFinder finder = new ReflectiveInstanceFinder();
        for(TClass type : finder.find(TClass.class)) {
            if((type.name().bundleId() == bundle) && !ignore.contains(type)) {
                boolean found = false;
                for(TypeInfo info : typeInfos) {
                    if(info.type == type) {
                        found = true;
                        break;
                    }
                }
                if(!found) {
                    throw new AssertionError("No TypeInfo for " + type.name());
                }
            }
        }
        List<Object[]> params = new ArrayList<>();
        for(TypeInfo info : typeInfos) {
            String name = info.type.name().unqualifiedName();
            Value min = ValueSources.valuefromObject(info.min, info.type.instance(true));
            Value max = ValueSources.valuefromObject(info.max, info.type.instance(true));
            params.add(new Object[] { name + "_min_min", min, min, 0 });
            params.add(new Object[] { name + "_min_max", min, max, -1 });
            params.add(new Object[] { name + "_max_min", max, min, 1 });
            params.add(new Object[] { name + "_max_max", max, max, 0 });
            if(info.zero != null) {
                Value zero = ValueSources.valuefromObject(info.zero, info.type.instance(true));
                params.add(new Object[] { name + "_min_zero", min, zero, -1 });
                params.add(new Object[] { name + "_zero_min", zero, min, 1 });
                params.add(new Object[] { name + "_zero_zero", zero, zero, 0 });
                params.add(new Object[] { name + "_zero_max", zero, max, -1 });
                params.add(new Object[] { name + "_max_zero", max, zero, 1 });
            }
        }
        return params;
    }


    private final String name;
    private final Value a;
    private final Value b;
    private final int expected;

    public TypeComparisonTestBase(String name, Value a, Value b, int expected) throws Exception {
        this.name = name;
        this.a = a;
        this.b = b;
        this.expected = expected;
    }


    @Test
    public void testCompare() {
        String desc = String.format("%s compareTo %s ", a, b);
        int actual = TClass.compare(a,b);
        if(expected == 0) {
            assertThat(desc, actual, equalTo(0));
        } else if(expected < 0) {
            assertThat(desc, actual, lessThan(0));
        } else {
            assertThat(desc, actual, greaterThan(0));
        }
    }
}
