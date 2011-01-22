package com.akiban.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents a failing test.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Failing
{
    /**
     * For parameterized tests, the optional list of parameterizations that are failing.
     *
     * This is ignored by non-parameterized class runners. For parameterized runners,
     * not specifying this value means that all parameterizations are failing.
     */
    String[] value() default {};
}
