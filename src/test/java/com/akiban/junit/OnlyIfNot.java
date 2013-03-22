
package com.akiban.junit;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Represents a condition for running a test.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OnlyIfNot
{
    /**
     * The method that should be invoked to see if this annotated method should be run
     * @return
     */
    String value();
}
