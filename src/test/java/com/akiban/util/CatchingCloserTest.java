/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

public class CatchingCloserTest {
    private class CtorException extends IOException {
    };

    private class ThrowItException extends IOException {
    };

    private class ClosingException extends IOException {
    };

    private class MyStream extends InputStream {
        private final boolean throwOnClose;
        private boolean isOpen = true;

        public MyStream(boolean throwFromCtor, boolean throwFromClose)
                throws IOException {
            if (throwFromCtor) {
                throw new CtorException();
            }
            this.throwOnClose = throwFromClose;
        }

        @Override
        public int read() {
            return -1;
        }

        public int throwIt() throws IOException {
            throw new ThrowItException();
        }

        @Override
        public void close() throws IOException {
            if (throwOnClose) {
                throw new ClosingException();
            }
            isOpen = false;
        }

        public boolean isOpen() {
            return isOpen;
        }
    }

    @Test
    public void nothingThrown() throws IOException {
        MyStream real = new MyStream(false, false);
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>(real);
        s.getCloseable().read();
        s.close();
        Assert.assertFalse(real.isOpen());
        Assert.assertNull(s.getCloseable());
    }

    @Test(expected = ThrowItException.class)
    public void throwOrigException_NoExceptionOnClose() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>(new MyStream(
                false, false));
        try {
            s.getCloseable().throwIt();
        } catch (IOException e) {
            s.caught(e);
        } finally {
            s.close();
        }
    }

    @Test(expected = ThrowItException.class)
    public void throwOrigException_WithExceptionOnClose() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>(new MyStream(
                false, true));
        try {
            s.getCloseable().throwIt();
        } catch (IOException e) {
            s.caught(e);
        } finally {
            s.close();
        }
    }

    @Test(expected = ClosingException.class)
    public void throwClosingException_WithExceptionOnClose() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>(new MyStream(
                false, true));
        try {
            s.getCloseable().throwIt();
        } catch (IOException e) {
            s.caught(e);
        } finally {
            s.close(CatchingCloser.Mode.THROW_CLOSE_EXCEPTION);
        }
    }

    @Test(expected = ThrowItException.class)
    public void throwClosingException_NoExceptionOnClose() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>(new MyStream(
                false, false));
        try {
            s.getCloseable().throwIt();
        } catch (IOException e) {
            s.caught(e);
        } finally {
            s.close(CatchingCloser.Mode.THROW_CLOSE_EXCEPTION);
        }
    }

    @Test(expected = CtorException.class)
    public void throwExceptionInCtor_OrigException() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>();
        try {
            s.set(new MyStream(true, true));
        } catch (IOException e) {
            s.caught(e);
        } finally {
            s.close();
        }
    }

    /**
     * We actually still want a CtorException, since there's only one exception
     * -- the one thrown by ctor
     * 
     * @throws IOException
     */
    @Test(expected = CtorException.class)
    public void throwExceptionInCtor_ClosingException() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>();
        try {
            s.set(new MyStream(true, true));
        } catch (IOException e) {
            s.caught(e);
        } finally {
            s.close(CatchingCloser.Mode.THROW_CLOSE_EXCEPTION);
        }
    }

    @Test
    public void catchGenericException() {
        boolean caughtIndexOutOfBounds = false;
        CatchingCloser<InputStream> c = new CatchingCloser<InputStream>();
        try {
            new LinkedList<Object>().get(50);
        } catch (IndexOutOfBoundsException indexException) {
            c.caught(indexException);
            caughtIndexOutOfBounds = true;
        }
        Exception thrown = null;
        try {
            c.close();
        } catch (IOException e) {
            thrown = e;
        }

        Assert.assertTrue(caughtIndexOutOfBounds);
        Assert.assertNotNull(thrown);
        Assert.assertNotNull(thrown.getCause());
    }

    @Test(expected = ThrowItException.class)
    public void catchGenericIOException() throws Exception {
        CatchingCloser<MyStream> c = new CatchingCloser<MyStream>();
        try {
            c.set(new MyStream(false, false));
            c.getCloseable().throwIt();
        } catch (Exception e) {
            c.caught(e); // this uses CatchingCloser.caught(Exception), since
                         // method overriding is determined at compile-time
        }

        Exception thrown = null;
        try {
            c.close();
        } catch (IOException e) {
            thrown = e;
        }

        Assert.assertNotNull(thrown);
        Assert.assertNull(thrown.getCause());
        throw thrown;
    }

    @Test(expected = NullPointerException.class)
    public void nullArgToCtor() {
        new CatchingCloser<InputStream>(null);
    }

    @Test(expected = NullPointerException.class)
    public void nullArgToSet() {
        new CatchingCloser<InputStream>().set(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setCalledAfterNondefaultCtor() throws IOException {
        new CatchingCloser<InputStream>(new MyStream(false, false)).set(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void setCalledTwice() throws IOException {
        CatchingCloser<InputStream> c = new CatchingCloser<InputStream>();
        c.set(new MyStream(false, false));
        c.set(new MyStream(false, false));
    }

    @Test
    public void setCalledTwiceButClosed() throws IOException {
        CatchingCloser<InputStream> c = new CatchingCloser<InputStream>();
        c.set(new MyStream(false, false));
        c.close();
        c.set(new MyStream(false, false));
    }

    @Test(expected = NullPointerException.class)
    public void passingNullToClose() throws IOException {
        CatchingCloser<MyStream> s = new CatchingCloser<MyStream>(new MyStream(
                false, false));
        s.getCloseable().read();
        s.close(null);
    }
}
