package com.akiban.cserver.service;

/**
 * Interface for Services that require an additional setup step after
 * all the Services have been started and registered.
 * @author peter
 *
 */
public interface AfterStart {

    void afterStart() throws Exception;
}
