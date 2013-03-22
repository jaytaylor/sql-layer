
package com.akiban.server.types3;

import com.akiban.util.ArgumentValidation;

import java.util.UUID;

public final class TBundleID {

    public String name() {
        return name;
    }

    public UUID uuid() {
        return uuid;
    }

    // object interface

    @Override
    public int hashCode() {
        return uuid().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != TBundleID.class)
            return false;
        TBundleID other = (TBundleID) obj;
        return uuid.equals(other.uuid);
    }

    @Override
    public String toString() {
        return name;
    }

    public TBundleID(String name, String uuid) {
        this(name, UUID.fromString(uuid));
    }

    public TBundleID(String name, UUID uuid) {
        ArgumentValidation.notNull("name", name);
        ArgumentValidation.notNull("uuid", uuid);
        if (!name.matches(VALID_NAME))
            throw new IllegalNameException("name must contain only letters: " + name);
        this.name = name.toUpperCase();
        this.uuid = uuid;
    }

    private final String name;
    private final UUID uuid;
    private static final String VALID_NAME = "[a-zA-Z]+";
}
