/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service;

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
    void crash() throws Exception;

}
