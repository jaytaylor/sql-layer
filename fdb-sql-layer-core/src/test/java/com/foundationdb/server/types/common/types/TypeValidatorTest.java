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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class TypeValidatorTest
{
    final TypesRegistry typesRegistry =
        TypesRegistryServiceImpl.createRegistryService().getTypesRegistry();

    @Test
    public void testTypeSupported() throws Exception {
        assertTrue(isTypeSupported("MCOMPAT", "int"));
        assertTrue(isTypeSupported("MCOMPAT", "varchar"));
        assertTrue(isTypeSupported("MCOMPAT", "text"));
//        assertTrue(isTypeSupported("MCOMPAT", "blob"));
        assertFalse(isTypeSupported("AKSQL", "result set"));
    }

    @Test
    public void testTypeSupportedAsIndex() throws Exception {
        assertTrue(isTypeSupportedAsIndex("MCOMPAT", "int"));
        assertTrue(isTypeSupportedAsIndex("MCOMPAT", "varchar"));
        assertFalse(isTypeSupportedAsIndex("MCOMPAT", "text"));
//        assertFalse(isTypeSupportedAsIndex("MCOMPAT", "blob"));
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
                assertTrue(t1+"->"+t2, canTypesBeJoined("MCOMPAT", t1, "MCOMPAT", t2));
                assertEquals(t1U + "->" + t2, !t1UIsBigint, canTypesBeJoined("MCOMPAT", t1U, "MCOMPAT", t2));
                assertEquals(t1 + "->" + t2U, !t2UIsBigint, canTypesBeJoined("MCOMPAT", t1, "MCOMPAT", t2U));
                assertEquals(t1U+"->"+t2U, (t1UIsBigint == t2UIsBigint), canTypesBeJoined("MCOMPAT", t1U, "MCOMPAT", t2U));
            }
        }
        // Check a few that cannot be
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "varchar"));
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "timestamp"));
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "decimal"));
        assertFalse(canTypesBeJoined("MCOMPAT", "int", "MCOMPAT", "double"));
        assertFalse(canTypesBeJoined("MCOMPAT", "char", "MCOMPAT", "binary"));
    }

    protected boolean isTypeSupported(String bundle, String name) {
        TClass tc = typesRegistry.getTypeClass(bundle, name);
        assertNotNull(name, tc);
        return TypeValidator.isSupportedForColumn(tc);
    }

    protected boolean isTypeSupportedAsIndex(String bundle, String name) {
        TClass tc = typesRegistry.getTypeClass(bundle, name);
        assertNotNull(name, tc);
        return TypeValidator.isSupportedForIndex(tc);
    }

    protected boolean canTypesBeJoined(String b1, String t1, String b2, String t2) {
        TClass c1 = typesRegistry.getTypeClass(b1, t1);
        assertNotNull(t1, c1);
        TClass c2 = typesRegistry.getTypeClass(b2, t2);
        assertNotNull(t2, c2);
        return TypeValidator.isSupportedForJoin(c1, c2);
    }
}
