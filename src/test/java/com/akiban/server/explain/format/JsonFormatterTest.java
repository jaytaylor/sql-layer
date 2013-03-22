
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
