
package com.akiban.server.types3.texpressions;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface SerializeAs {
    Serialization value();
}
