package com.akiban.ais.gwtutils;

public interface GwtLogFactory
{
    /**
     * Get a logger for the appropriate class. Users should not make any assumption about this logger, other than
     * that it will not throw any exceptions. In particular, they shouldn't make assumptions about its identity;
     * if you want to return the same logger for each class, that's fine.
     * @param clazz the class for which we want logging
     * @return the logger
     */
    GwtLogger getLogger(Class<?> clazz);
}
