
package com.akiban.server.types3;

public final class TInstanceBuilder {

    public TInstanceBuilder setNullable(boolean nullable) {
        if (workingCopy != null) {
            if (workingCopy.nullability() == nullable)
                return this;
            copyFromWorking();
        }
        this.nullable = nullable;
        return this;
    }

    public TInstanceBuilder setAttribute(Attribute attribute, int value) {
        if (workingCopy != null) {
            if (workingCopy.attribute(attribute) == value)
                return this;
            copyFromWorking();
        }
        switch (attribute.ordinal()) {
        case 0:
            attr0 = value;
            break;
        case 1:
            attr1 = value;
            break;
        case 2:
            attr2 = value;
            break;
        case 3:
            attr3 = value;
            break;
        }
        return this;
    }

    public TInstanceBuilder copyFrom(TInstance tInstance) {
        if (tInstance.typeClass() != orig.typeClass() || tInstance.enumClass() != orig.enumClass())
            throw new IllegalArgumentException("can't copy " + tInstance + " to a builder based on " + orig);
        this.workingCopy = tInstance;
        return this;
    }

    public TInstance get() {
        if (workingCopy == null) // all of our mutations were noops, so we can just return the old TInstance
            workingCopy = TInstance.create(orig, attr0, attr1, attr2, attr3, nullable);
        return workingCopy;
    }

    public TInstanceBuilder(TInstance orig) {
        this.orig = orig;
        this.workingCopy = orig;
        this.nullable = orig.nullability();
    }

    private void copyFromWorking() {
        assert nullable == workingCopy.nullability();
        attr0 = workingCopy.attrByPos(0);
        attr1 = workingCopy.attrByPos(1);
        attr2 = workingCopy.attrByPos(2);
        attr3 = workingCopy.attrByPos(3);
        workingCopy = null;
    }

    private final TInstance orig;
    private int attr0;
    private int attr1;
    private int attr2;
    private int attr3;
    private boolean nullable;
    private TInstance workingCopy;
}
