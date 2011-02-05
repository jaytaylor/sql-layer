package com.akiban.junit;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public final class OnlyIfUsageTest {
    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        builder.add("foo", "foo");
        builder.add("bar", "bar");
        return builder.asList();
    }

    private final String string;

    public OnlyIfUsageTest(String string) {
        this.string = string;
    }

    @Test @OnlyIf("isFoo")
    public void equalsFoo() {
        assertEquals("string", "foo", string);
    }

    @Test
    public void stringNotNull() {
        assertNotNull("string", string);
    }

    public boolean isFoo() {
        return "foo".equals(string);
    }
}
