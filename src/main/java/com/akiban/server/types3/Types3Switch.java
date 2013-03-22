
package com.akiban.server.types3;

public class Types3Switch {
    public static final boolean DEFAULT = Boolean.parseBoolean(System.getProperty("newtypes", "true"));

    public static volatile boolean ON = DEFAULT;
}
