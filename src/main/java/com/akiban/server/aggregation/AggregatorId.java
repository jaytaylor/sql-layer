
package com.akiban.server.aggregation;

import com.akiban.server.types.AkType;
import com.akiban.util.ArgumentValidation;

public final class AggregatorId {

    public AggregatorId(String name, AkType type) {
        this.name = name.toUpperCase();
        this.type = type;
        ArgumentValidation.notNull("name", this.name);
        ArgumentValidation.notNull("type", this.type);
    }

    public String name() {
        return name;
    }

    public AkType type() {
        return type;
    }

    // object interface

    @Override
    public String toString() {
        return name + " -> " + type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatorId that = (AggregatorId) o;
        return name.equals(that.name) && type == that.type;

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    private final String name;
    private final AkType type;
}
