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

package com.foundationdb.server.types.aksql.aktypes;

import com.foundationdb.server.types.aksql.AkParsers;
import com.foundationdb.server.types.aksql.AkCategory;
import com.foundationdb.server.types.common.TFormatter;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.sql.types.TypeId;

/**
 * 
 * Implement Main's bool type which is a Java's primitive boolean
 */
public class AkBool
{
    public static final NoAttrTClass INSTANCE 
            = new NoAttrTClass(AkBundle.INSTANCE.id(), "boolean", AkCategory.LOGIC, TFormatter.FORMAT.BOOL, 1, 1, 1,
                               UnderlyingType.BOOL, AkParsers.BOOLEAN, 5, TypeId.BOOLEAN_ID);
}
