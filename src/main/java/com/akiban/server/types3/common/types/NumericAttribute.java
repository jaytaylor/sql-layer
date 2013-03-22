
package com.akiban.server.types3.common.types;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.texpressions.Serialization;
import com.akiban.server.types3.texpressions.SerializeAs;

public enum NumericAttribute implements Attribute
{
    /**
     * The display width [M] of a number
     * (unrelated to the range of its value)
     */
    @SerializeAs(Serialization.LONG_1) WIDTH
}
