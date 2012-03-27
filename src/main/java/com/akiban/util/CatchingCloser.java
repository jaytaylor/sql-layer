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

import java.io.Closeable;
import java.io.IOException;

/**
 * <p>
 * Convenience wrapper for Closeable's close().
 * </p>
 * 
 * <p>
 * You'll often want to catch an IOException and then in the finally clause,
 * close the Closeable. But what to do with the original exception? This
 * simplifies that code.
 * </p>
 * 
 * <p>
 * The pattern is to catch the IOException using {@link #caught(IOException)},
 * and then close this {@linkplain CatchingCloser} in the finally clause. If
 * there was only one exception thrown during this process (either during
 * catching or during closing, but not during both), that exception is thrown in
 * {@link #close()}. If an exception was thrown in both processes, the default
 * behavior is to throw the earlier exception; you can control this using the
 * more specifid {@link #close(Mode)}.
 * </p>
 * 
 * <p>
 * This will turn the following mess:
 * </p>
 * 
 * <pre>
 * IOException thrown = null;
 * InputStream is;
 * try {
 *     is = getMyInputStream(); // and possibly throw IOException in the process
 *     is.read();
 * } catch (IOException e) {
 *     thrown = e;
 * } finally {
 *     try {
 *         is.close();
 *     } catch (IOException e) {
 *         thrown = (thrown == null) ? e : thrown;
 *     }
 *     if (thrown != null) {
 *         throw thrown;
 *     }
 * }
 * </pre>
 * 
 * <p>
 * ...into this:
 * </p>
 * 
 * <pre>
 * CatchingCloser&lt;InputStream&gt; is = new CatchingCloser&lt;InputStream&gt;();
 * try {
 *     is.set(getMyInputStream()); // and possibly throw IOException in the process
 *     is.getCloseable().read();
 * } catch (IOException e) {
 *     is.caught(e);
 * } finally {
 *     is.close();
 * }
 * </pre>
 */
public final class CatchingCloser<T extends Closeable> implements Closeable {
    public enum Mode {
        THROW_ORIGINAL, THROW_CLOSE_EXCEPTION
    }

    private T closeable;
    private IOException caught = null;

    /**
     * <p>
     * Creates a {@linkplain CatchingCloser} which will not close anything.
     * </p>
     * 
     * <p>
     * You can set a target later using {@link #set(Closeable)}. If you don't,
     * and you try to close this instance, it will silently succeed. This may
     * represent an uncaught programming bug.
     * </p>
     */
    public CatchingCloser() {
        this.closeable = null;
    }

    public CatchingCloser(T closeable) {
        set(closeable);
    }

    /**
     * Sets the target closeable.
     * 
     * @param closeable
     *            the new target
     * @throws IllegalArgumentException
     *             if the target has already been set but not yet closed
     */
    public void set(T closeable) {
        if (this.closeable != null) {
            throw new IllegalArgumentException("target already set to: "
                    + this.closeable.toString());
        }
        if (closeable == null) {
            throw new NullPointerException();
        }
        this.closeable = closeable;
    }

    /**
     * Catches an IOException, which may then be thrown during {@link #close()}.
     * 
     * @param e
     */
    public void caught(IOException e) {
        this.caught = e;
    }

    /**
     * <p>
     * Catches an Exception, which may then be thrown during {@link #close()}.
     * </p>
     * 
     * <p>
     * If {@code e} is an {@code IOException}, this behaves exactly like
     * {@link #caught(IOException)}. Otherwise, this method wraps the given
     * exception in an IOException.
     * </p>
     * 
     * @param e
     */
    public void caught(Exception e) {
        if (e instanceof IOException) {
            this.caught = (IOException) e;
        } else {
            this.caught = new IOException(e);
        }
    }

    /**
     * <p>
     * Closes the closeable.
     * </p>
     * 
     * <p>
     * This also lets you call {@link #set(Closeable)} again.
     * </p>
     * 
     * @param mode
     *            may not be null
     * @throws IOException
     *             if there was an original exception, or if closing this
     *             closeable threw an exception
     */
    public void close(Mode mode) throws IOException {
        if (mode == null) {
            throw new NullPointerException();
        }

        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                if (caught != null) {
                    caught = mode == Mode.THROW_ORIGINAL ? caught : e;
                } else {
                    caught = e;
                }
            }
        }
        if (caught != null) {
            throw caught;
        }
        closeable = null;
    }

    /**
     * Closes the closeable
     * 
     * @throws IOException
     *             if there was an original exception, or if closing this
     *             closeable threw an exception
     */
    @Override
    public void close() throws IOException {
        close(Mode.THROW_ORIGINAL);
    }

    /**
     * Returns the original closeable.
     * 
     * @return
     */
    public T getCloseable() {
        return closeable;
    }
}
