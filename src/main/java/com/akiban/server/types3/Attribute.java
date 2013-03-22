
package com.akiban.server.types3;

public interface Attribute
{
    public static enum NONE implements Attribute{};
    
    String name();
    int ordinal();
}
