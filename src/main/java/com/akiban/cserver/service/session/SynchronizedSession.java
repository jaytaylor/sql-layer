package com.akiban.cserver.service.session;

public final class SynchronizedSession implements Session {
    private final Session session;
    private final Object monitor;

    public SynchronizedSession(Session session) {
        this.session = session;
        this.monitor = new Object();
    }

    public interface SessionRunnable<T,E extends Exception> {
        T run(SynchronizedSession self) throws E;
    }

    public <T,E extends Exception, S extends SessionRunnable<T,E>> T run(S runnable) throws E {
        synchronized (monitor) {
            return runnable.run(this);
        }
    }

    @Override
    public <T> T get(String module, Object key) {
        synchronized (monitor) {
            return (T) session.get(module, key);
        }
    }

    @Override
    public <T> T put(String module, Object key, T item) {
        synchronized (monitor) {
            return session.put(module, key, item);
        }
    }

    @Override
    public <T> T remove(String module, Object key) {
        synchronized (monitor) {
            return (T) session.remove(module, key);
        }
    }

    @Override
    public boolean isCanceled() {
        synchronized (monitor) {
            return session.isCanceled();
        }
    }

    @Override
    public void close() {
        synchronized (monitor) {
            session.close();
        }
    }

    @Override
    public boolean equals(Object obj) {
        synchronized (monitor) {
            return session.equals(obj);
        }
    }

    @Override
    public int hashCode() {
        synchronized (monitor) {
            return session.hashCode();
        }
    }
}
