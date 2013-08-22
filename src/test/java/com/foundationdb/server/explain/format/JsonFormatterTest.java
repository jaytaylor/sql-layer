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

package com.foundationdb.server.explain.format;

import com.foundationdb.server.explain.*;

import org.junit.*;
import static org.junit.Assert.assertEquals;

public class JsonFormatterTest 
{
    @Test
    public void testDescribe_Explainer() {
        PrimitiveExplainer s = PrimitiveExplainer.getInstance("string");
        PrimitiveExplainer l = PrimitiveExplainer.getInstance(123);
        PrimitiveExplainer n = PrimitiveExplainer.getInstance(3.14);
        PrimitiveExplainer b = PrimitiveExplainer.getInstance(true);

        Attributes a = new Attributes();
        a.put(Label.NAME, PrimitiveExplainer.getInstance("TEST"));
        a.put(Label.OPERAND, s);
        a.put(Label.OPERAND, l);
        a.put(Label.OPERAND, n);
        a.put(Label.OPERAND, b);
        CompoundExplainer c = new CompoundExplainer(Type.FUNCTION, a);

        CompoundExplainer c2 = new CompoundExplainer(Type.EXTRA_INFO);
        c2.addAttribute(Label.COST, PrimitiveExplainer.getInstance("a lot"));
        c.addAttribute(Label.EXTRA_TAG, c2);

        String expected = 
            "{\n" +
            "  \"type\" : \"function\",\n" +
            "  \"operand\" : [ \"string\", 123, 3.14, true ],\n" +
            "  \"extra_tag\" : [ {\n" +
            "    \"type\" : \"extra_info\",\n" +
            "    \"cost\" : [ \"a lot\" ]\n" +
            "  } ],\n" +
            "  \"name\" : [ \"TEST\" ]\n" +
            "}";

        JsonFormatter f = new JsonFormatter();
        assertEquals(expected, f.format(c));
    }
}
