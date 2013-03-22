
package com.akiban.server.entity.model;

import java.util.UUID;

public final class Util {

    public static UUID parseUUID(String string) {
        if (string == null)
            return null;
        UUID uuid;
        try {
            uuid = UUID.fromString(string);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalEntityDefinition("invalid uuid");
        }
        return uuid;
    }
}
