package com.akiban.server.mttests.mthapi.common;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class HapiValidationErrorTest {
    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        for (HapiValidationError.Reason reason : HapiValidationError.Reason.values()) {
            builder.add(reason.name(), reason);
        }
        return builder.asList();
    }

    private final HapiValidationError.Reason reason;

    public HapiValidationErrorTest(HapiValidationError.Reason reason) {
        this.reason = reason;
    }

    @Test(expected=HapiValidationError.class)
    public void laundryTest() {
        try {
            HapiValidationError.fail(reason, "failure");
        } catch (HapiValidationError e) {
            assertEquals("name/reason mismatch",
                    (reason.name().replaceAll("_", "")+"Error").toUpperCase(),
                    e.getClass().getSimpleName().toUpperCase()
            );
            throw e;
        }
    }
}
