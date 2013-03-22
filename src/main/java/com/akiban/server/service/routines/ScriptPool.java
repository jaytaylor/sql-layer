
package com.akiban.server.service.routines;

public interface ScriptPool<T>
{
    public T get();
    public void put(T elem, boolean success);
}
