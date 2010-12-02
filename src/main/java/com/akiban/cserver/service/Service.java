package com.akiban.cserver.service;

/**
 * Defines the interface by which services can be started and stopped. Each Service is tied to a specific service
 * class via the generic argument. If a Service can't (or shouldn't) be tied to a class, its generic parameter should
 * be Void, and attempts to cast it (via this interface) should throw a NotCastableException.
 * @param <T> the service's class
 */
public interface Service<T>
{
    public class NotCastableException extends RuntimeException
    {}
    /**
     * Returns this service as its T type. Implementations should always just <tt>return this</tt>, unless T is
     * <tt>Void</tt>, in which case they should throw a NotCastableException
     * @return "this"
     * @throws NotCastableException if T is null
     */
    T cast();
    void start() throws Exception;
    void stop() throws Exception;
}
