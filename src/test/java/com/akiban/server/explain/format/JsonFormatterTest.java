/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.explain.format;

import com.akiban.server.explain.*;

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
