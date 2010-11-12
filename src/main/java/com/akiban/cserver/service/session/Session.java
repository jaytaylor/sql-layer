package com.akiban.cserver.service.session;

public interface Session {
    <T> T get(String module, Object key);
    <T> T put(String module, Object key, T item);
    <T> T remove(String module, Object key);
    
    /**
     * A session may be marked as canceled.
     * 
     * For example, a connection may close and we no longer need to execute a long running
     * operation. Code that runs for a long time may look at the session periodically and then
     * stop its execution if the session is canceled.
     * 
     * @return true if the session is canceled, false otherwise.
     */
    boolean isCanceled();
}
