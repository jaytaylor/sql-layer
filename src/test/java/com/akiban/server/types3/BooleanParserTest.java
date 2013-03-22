
package com.akiban.server.types3;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public final class BooleanParserTest {

    @TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        param(builder, "1", true);
        param(builder, "1-1", true);
        param(builder, "1.1", true);
        param(builder, "1.1.1.1.1", true);
        param(builder, ".1", true); // this is weird. ".1" as tinyint is 0, and booleans in mysql are tinyint. but
                                    // (false OR ".1") results in a tinyint 1 (ie, true). Gotta love MySQL.
        param(builder, "0.1", true);
        param(builder, "-1", true);
        param(builder, "-1.1-a", true);
        param(builder, ".-1", false);
        param(builder, "-.1", true);
        param(builder, "-..1", false);
        param(builder, "1a", true);
        param(builder, "a1", false); // MySQL doesn't believe in steak sauce
        param(builder, "0", false);
        param(builder, "0.0", false);
        param(builder, "true", false);

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder, String string, boolean expected) {
        builder.add(string, string, expected);
    }

    public BooleanParserTest(String string, boolean expected) {
        this.string = string;
        this.expected = expected;
    }

    private String string;
    private boolean expected;

    @Test
    public void checkParse() {
        PValueSource source = new PValue(MString.varcharFor(string), string);
        PValue target = new PValue(AkBool.INSTANCE.instance(true));
        TParsers.BOOLEAN.parse(null, source, target);
        Boolean actual = target.isNull() ? null : target.getBoolean();
        assertEquals(string, Boolean.valueOf(expected), actual);
    }
}
