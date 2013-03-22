package com.akiban.util;

public final class Enums {
    public static <T extends Enum<T>> int ordinalOf(Class<T> enumClass, String name) {
        name = name.toUpperCase();
        Enum<T> enumVal = Enum.valueOf(enumClass, name);
        return enumVal.ordinal();
    } 
}
