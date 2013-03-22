
package com.akiban.server.service;

/**
 * Defines the interface by which services can be started and stopped. Each Service is tied to a specific service
 * class via the generic argument.
 */
public interface Service {
    void start();
    void stop();
    void crash();
}
