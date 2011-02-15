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

package com.akiban.server.service.session;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

public final class SessionServiceImplTest {
    private void fullLifecycle(boolean removeBeforeClosing) {
        final DummySessionFactory factory = new DummySessionFactory();
        final SessionServiceImpl service = new SessionServiceImpl(factory);

        final SessionHandle mySessionHandle = new DummySessionHandle();
        service.createSession(mySessionHandle);
        assertStats(service, 1, 0, 0);

        final Session session = service.acquireSession(mySessionHandle);
        assertStats(service, 1, 1, 0);
        assertEquals("created size", 1, factory.getCreated().size());
        assertEquals("created[0]", session, factory.getCreated().get(0));

        if (removeBeforeClosing) {
            service.releaseSession(mySessionHandle);
            assertStats(service, 1, 0, 0);
        }

        service.destroySession(mySessionHandle);
        assertStats(service, 1, 0, 1);
    }

    @Test
    public void unacquiredSessionDestroyed() {
        fullLifecycle(false);
    }

    @Test
    public void acquiredSessionDestroyed() {
        fullLifecycle(false);
    }

    @Test(expected=SessionException.class)
    public void createSessionTwiceForHandle() {
        final SessionServiceImpl service = createService();
        final SessionHandle handle = new DummySessionHandle();
        try {
            service.createSession(handle);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        try {
            service.createSession(handle);
        } catch (SessionException e) {
            assertStats(service, 1, 0, 0);
            throw e;
        }
    }

    @Test(expected=SessionException.class)
    public void acquireSessionTwice() {
        final SessionServiceImpl service;
        final SessionHandle mySessionHandle;
        try {
            final DummySessionFactory factory = new DummySessionFactory();
            service = new SessionServiceImpl(factory);
            mySessionHandle = new DummySessionHandle();
            service.createSession(mySessionHandle);
            assertStats(service, 1, 0, 0);

            final Session session = service.acquireSession(mySessionHandle);
            assertStats(service, 1, 1, 0);
            assertEquals("created size", 1, factory.getCreated().size());
            assertEquals("created[0]", session, factory.getCreated().get(0));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        service.acquireSession(mySessionHandle);
    }

    @Test(expected=SessionException.class)
    public void releaseUnacquiredSession() {
        final SessionServiceImpl service;
        final SessionHandle mySessionHandle;
        try {
            service = new SessionServiceImpl( new DummySessionFactory() );
            mySessionHandle = new DummySessionHandle();
            service.createSession(mySessionHandle);
            assertStats(service, 1, 0, 0);
        } catch(Throwable t) {
            throw new RuntimeException(t);
        }
        service.releaseSession(mySessionHandle);
    }

    private static SessionServiceImpl createService() {
        return createService(new DummySessionFactory());
    }

    private static SessionServiceImpl createService(SessionFactory factory) {
        try {
            return new SessionServiceImpl( factory );
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test(expected=SessionException.class)
    public void acquireUnknownSession() {
        createService().acquireSession( new DummySessionHandle() );
    }

    @Test(expected=SessionException.class)
    public void releaseUnknownSession() {
        createService().releaseSession( new DummySessionHandle() );
    }

    @Test(expected=SessionException.class)
    public void destroyUnknownSession() {
        createService().destroySession( new DummySessionHandle() );
    }

    @Test(expected=IllegalArgumentException.class)
    public void createNullHandle() {
        createService().createSession(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void acquireNullHandle() {
        createService().acquireSession(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void releaseNullHandle() {
        createService().releaseSession(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void destroyNullHandle() {
        createService().destroySession(null);
    }

    @Test(expected=IllegalArgumentException.class)
    public void factoryIsNull() {
        new SessionServiceImpl(null);
    }

    @Test(expected=SessionException.class)
    public void factoryReturnsNull() {
        final SessionServiceImpl service = createService(new SessionFactory() {
            @Override
            public Session createSession() {
                return null;
            }
        });
        try {
            service.createSession(new DummySessionHandle());
        } catch (SessionException e) {
            assertStats(service, 0, 0, 0);
            throw e;
        }
    }

    @Test(expected=SessionException.class)
    public void factoryThrowsException() {
        final SessionServiceImpl service = createService( new SessionFactory() {
            @Override
            public Session createSession() {
                throw new UnsupportedOperationException();
            }
        });
        try {
            service.createSession(new DummySessionHandle());
        } catch (SessionException e) {
            assertStats(service, 0, 0, 0);
            throw e;
        }
    }

    /**
     * This test makes sure that the SessionFactory's alien call, to SessionFactory.createSession(), can't
     * lock the whole service.
     *
     * We create a custom factory whose first call to createSession() will block until we count down a latch. We
     * then start two threads, each of which creates a session. We wait to make sure that the first thread blocks,
     * and then we start the second thread and confirm that it finishes correctly. We then unlatch the first thread
     * and confirm that it also finishes correctly.
     * @throws InterruptedException from threads
     */
    @Test(timeout=10000)
    public void serviceDoesNotWaitOnFactory() throws InterruptedException {
        final CountDownLatch factoryMayContinueLatch = new CountDownLatch(1);
        final CountDownLatch firstRunRecognized = new CountDownLatch(1);
        // Factory whose first invocation will block until the latch is triggered
        final SessionFactory factory = new SessionFactory() {
            private boolean firstRun = true;
            @Override
            public Session createSession() {
                final boolean isFirstRun;
                synchronized (this) {
                    isFirstRun = firstRun;
                    firstRun = false;
                }
                if (isFirstRun) {
                    try {
                        firstRunRecognized.countDown();
                        factoryMayContinueLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                return new DummySession();
            }
        };
        final SessionServiceImpl service = new SessionServiceImpl(factory);
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                service.createSession( new DummySessionHandle());
            }
        };

        Thread thread2 = new Thread() {
            @Override
            public void run() {
                service.createSession( new DummySessionHandle());
            }
        };

        thread1.start();
        firstRunRecognized.await();
        while (thread1.getState() != Thread.State.WAITING) {
            // JUnit will handle the timeout
        }

        thread2.start();
        thread2.join();
        assertStats(service, 1, 0, 0);

        factoryMayContinueLatch.countDown();
        thread1.join();
        assertStats(service, 2, 0, 0);
    }

    private static void assertStats(SessionServiceImpl service, int expCreated, int expActive, int expClosed) {
        SessionServiceMXBean bean = service.getBean();
        assertEquals("created", expCreated, bean.getSessionsCreated());
        assertEquals("active", expActive, bean.getSessionsActive());
        assertEquals("closed", expClosed, bean.getSessionsClosed());
    }

    private static class DummySessionHandle implements SessionHandle {
    }

    private static class DummySessionFactory implements SessionFactory {
        private final List<DummySession> created = Collections.synchronizedList( new ArrayList<DummySession>() );
        @Override
        public DummySession createSession() {
            DummySession ret = new DummySession();
            created.add(ret);
            return ret;
        }

        public List<DummySession> getCreated() {
            return Collections.unmodifiableList(created);
        }
    }

    private static class DummySession implements Session
    {
        @Override
        public <T> T get(Class<?> module, Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T put(Class<?> module, Object key, T item) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T remove(Class<?> module, Object key) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        public boolean isCanceled()
        {
            return false;
        }
        
        @Override
        public void close()
        {
            throw new UnsupportedOperationException();
        }
    }
}
