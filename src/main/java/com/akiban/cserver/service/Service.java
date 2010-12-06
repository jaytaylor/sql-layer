package com.akiban.cserver.service;

/**
 * Defines the interface by which services can be started and stopped. Each Service is tied to a specific service
 * class via the generic argument.
 * @param <T> the service's class
 */
public interface Service<T>
{
    public class NotCastableException extends RuntimeException
    {}
    /**
     * Returns this service as its T type. Implementations should always just <tt>return this</tt>.
     * @return "this"
     * @throws NotCastableException if T is null
     */
    T cast();

    /**
     * Returns the class of T.
     * @return the class that this method's cast() method will return
     */
    Class<T> castClass();

    void start() throws Exception;
    void stop() throws Exception;
}
