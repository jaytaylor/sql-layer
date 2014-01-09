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

package com.foundationdb.server.types.common.types;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.service.TypesRegistryServiceImpl;
import com.foundationdb.server.types.service.TypesRegistry;

import org.junit.Test;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TypeValidatorTest
{
    final TypesRegistry typesRegistry =
        TypesRegistryServiceImpl.createRegistryService().getTypesRegistry();

    @Test
    public void testTypeSupported() throws Exception {
        assertTrue(isTypeSupported("int"));
        assertTrue(isTypeSupported("varchar"));
        assertTrue(isTypeSupported("text"));
        assertTrue(isTypeSupported("blob"));
        assertFalse(isTypeSupported("result set"));
    }

    @Test
    public void testTypeSupportedAsIndex() throws Exception {
        assertTrue(isTypeSupportedAsIndex("int"));
        assertTrue(isTypeSupportedAsIndex("varchar"));
        assertFalse(isTypeSupportedAsIndex("text"));
        assertFalse(isTypeSupportedAsIndex("blob"));
    }

    @Test
    public void testTypesCanBeJoined() throws Exception {
        // Every time can be joined to itself
        for(TClass t : typesRegistry.getTypeClasses()) {
            assertTrue(t.toString(), TypeValidator.isSupportedForJoin(t, t));
        }
        // All int types can be joined together except bigint unsigned
        final String intTypeNames[] = {"tinyint", "smallint", "int", "mediumint", "bigint"};
        for(String t1 : intTypeNames) {
            String t1U = t1 + " unsigned";
            for(String t2 : intTypeNames) {
                String t2U = t2 + " unsigned";
                boolean t1UIsBigint = "bigint unsigned".equals(t1U);
                boolean t2UIsBigint = "bigint unsigned".equals(t2U);
                assertTrue(t1+"->"+t2, canTypesBeJoined(t1, t2));
                assertEquals(t1U + "->" + t2, !t1UIsBigint, canTypesBeJoined(t1U, t2));
                assertEquals(t1 + "->" + t2U, !t2UIsBigint, canTypesBeJoined(t1, t2U));
                assertEquals(t1U+"->"+t2U, (t1UIsBigint == t2UIsBigint), canTypesBeJoined(t1U, t2U));
            }
        }
        // Check a few that cannot be
        assertFalse(canTypesBeJoined("int", "varchar"));
        assertFalse(canTypesBeJoined("int", "timestamp"));
        assertFalse(canTypesBeJoined("int", "decimal"));
        assertFalse(canTypesBeJoined("int", "double"));
        assertFalse(canTypesBeJoined("char", "binary"));
    }

    protected boolean isTypeSupported(String name) {
        TClass tc = typesRegistry.getTypeClass(name);
        assertNotNull(name, tc);
        return TypeValidator.isSupportedForColumn(tc);
    }

    protected boolean isTypeSupportedAsIndex(String name) {
        TClass tc = typesRegistry.getTypeClass(name);
        assertNotNull(name, tc);
        return TypeValidator.isSupportedForIndex(tc);
    }

    protected boolean canTypesBeJoined(String t1, String t2) {
        TClass c1 = typesRegistry.getTypeClass(t1);
        assertNotNull(t1, c1);
        TClass c2 = typesRegistry.getTypeClass(t2);
        assertNotNull(t2, c2);
        return TypeValidator.isSupportedForJoin(c1, c2);
    }
}
