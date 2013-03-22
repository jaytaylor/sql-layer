
package com.akiban.server.types3.common.types;

import com.akiban.server.types3.Attribute;
import com.akiban.server.types3.texpressions.Serialization;
import com.akiban.server.types3.texpressions.SerializeAs;

public enum DoubleAttribute implements Attribute
{
    @SerializeAs(Serialization.LONG_1) PRECISION,
    @SerializeAs(Serialization.LONG_2) SCALE
}
