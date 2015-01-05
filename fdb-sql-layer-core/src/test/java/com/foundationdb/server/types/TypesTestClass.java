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

import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.UnderlyingType;

public class TypesTestClass extends NoAttrTClass {

    public TypesTestClass(String name) {
        super(bundle, name, TestCategory.ONLY, null, 1, 1, 1, UnderlyingType.INT_64, null, 64, null);
    }

    public enum TestClassCategory {
        ONLY
    }

    public static final TBundleID bundle = new TBundleID("testbundle", "8b298621-fed8-48ca-b33c-dd3a3905f72e");

    public enum TestCategory {
        ONLY
    }
}
