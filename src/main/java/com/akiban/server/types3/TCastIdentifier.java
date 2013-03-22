
package com.akiban.server.types3;

import com.google.common.base.Objects;

public final class TCastIdentifier {

    public TClass getSource() {
        return source;
    }

    public TClass getTarget() {
        return target;
    }

    @Override
    public String toString() {
        return source + " to " + target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TCastIdentifier that = (TCastIdentifier) o;

        return Objects.equal(this.source, that.source) && Objects.equal(this.target, that.target);
    }

    @Override
    public int hashCode() {
        int result = source != null ? source.hashCode() : 0;
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }

    public TCastIdentifier(TClass source, TClass target) {
        this.source = source;
        this.target = target;
    }

    public TCastIdentifier(TCast cast) {
        this(cast.sourceClass(), cast.targetClass());
    }

    private final TClass source;
    private final TClass target;
}
