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

package com.foundationdb.server.types.value;

import com.foundationdb.server.types.Attribute;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.BigDecimalWrapperImpl;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.common.types.TBigDecimal;
import org.junit.Test;

import java.math.BigDecimal;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import static com.foundationdb.server.types.value.ValueSources.*;

public class ValueSourcesTest
{
    private static void checkType(Class<? extends TClass> clazz,
                                  Attribute attr0, int value0,
                                  Attribute attr1, int value1,
                                  Value v) {
        TInstance type = v.getType();
        assertThat(type.typeClass(), is(instanceOf(clazz)));
        if(attr0 != null) {
            assertThat(attr0.name(), type.attribute(attr0), is(equalTo(value0)));
        }
        if(attr1 != null) {
            assertThat(attr1.name(), type.attribute(attr1), is(equalTo(value1)));
        }
    }

    private static void checkDecimal(int precision, int scale, Value v) {
        checkType(TBigDecimal.class, DecimalAttribute.PRECISION, precision, DecimalAttribute.SCALE, scale, v);
    }

    private static void checkDecimal(int precision, int scale, String decimalStr) {
        BigDecimalWrapper wrapper = new BigDecimalWrapperImpl(new BigDecimal(decimalStr));
        checkDecimal(precision, scale, fromObject(wrapper.asBigDecimal()));
        checkDecimal(precision, scale, fromObject(wrapper));
    }

    @Test
    public void fromObjectBigDecimals() {
        checkDecimal(6, 3, "-123.456");
        checkDecimal(2, 1, "-1.0");
        checkDecimal(2, 2, ".00");
        checkDecimal(1, 1, ".0");
        checkDecimal(1, 1, "0.0");
        checkDecimal(1, 0, "0.");
        checkDecimal(1, 0, "00.");
        checkDecimal(4, 4, "0.0005");
        checkDecimal(2, 1, "1.0");
        checkDecimal(6, 3, "123.456");
    }
}