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
import java.util.LinkedList;
import java.util.List;

/**
 * <p>Closes various Closeable instances.</p>
 * <p>Use this when you need to close multiple items, and don't want a bunch of embedded try-finally clauses.
 * For instance, instead of:</p>
 * <pre>   MyCloseable first = getFirstCloseable(); // if this throws something, there's nothing to close
 * AnotherCloseable second = null;
 * try{
 *   first.somethingThatMayThrowSomething();
 *   second = getSecondCloseable();
 *   second.somethingElse( first );
 *   ...
 * }
 * finally {
 *   Throwable thrown = null;
 *   try { first.close(); }
 *   catch (IOException e) { thrown = e; }
 *   if (second != null) {
 *     try { second.close(); }
 *     catch (IOException e) { if (thrown != null) thrown = e; }
 *   }
 *   if (thrown != null) throw thrown;
 * }</pre>
 * ... you can just write:
 * <pre>   MultiCloser closer = new MultiCloser();
 * try {
 *   MyCloseable first = closer.register( getFirstCloseable() );
 *   first.somethingThatMayThrowSomething();
 *   AnotherCloseable second = closer.register( getSecondCloseable() );
 *   second.somethingElse( first );
 *   ...
 * }
 * finally {
 *   closer.close();
 * }</pre>
 *
 * <p>If note that {@linkplain #register(Closeable)} returns the instance you pass to it, so it's easy to register
 * and assign Closeables in one step, as above. If the act of getting the Closeable itself throws an exception,
 * the closeable will not be registered (and thus won't be closed), which is the desired behavior.</p>
 *
 * <p>If multiple Closeables throw exceptions during {@linkplain #close()}, the default behavior is to throw the
 * first exception.</p>
 */
public final class MultiCloser implements Closeable
{
    private final List<Closeable> closeables = new LinkedList<Closeable>();
    private final List<Iterable<? extends Closeable>> iterables = new LinkedList<Iterable<? extends Closeable>>();
    private Throwable explicitThrow = null;
    private boolean printExtraThrowables = true;

    /**
     * <p>A convenience class for when you have closing behavior that can throw exceptions other than IOException.</p>
     *
     * <p>The {@link #customClose()} method can throw any checked Exception. The class's {@linkplain #close()} method
     * runs {@link #customClose()}, propagating IOExceptions directly, Exceptions wrapped in an IOException, and
     * all other Throwables wrapped in a RuntimeException.
     */
    public abstract static class CustomCloseable implements Closeable
    {
        protected abstract void customClose() throws Exception;
        @Override
        public final void close() throws IOException
        {
            try
            {
                customClose();
            }
            catch (IOException e)
            {
                throw e;
            }
            catch (RuntimeException t)
            {
                throw new RuntimeException(t);
            }
            catch (Exception e)
            {
                throw new IOException(e);
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        }
    }

    /**
     * Register a closeable, and return it.
     * @param closeable an item to close in {@link #close()}
     * @param <T> a Closeable
     * @return the closeable you passed in; lets you easily declare and register a new object in one line
     * @throws IllegalArgumentException if <tt>closeable == this</tt>
     */
    public <T extends Closeable> T register(T closeable)
    {
        if (closeable == this)
        {
            throw new IllegalArgumentException("multicloser can't close itself; would cause infinite recursion");
        }
        closeables.add(closeable);
        return closeable;
    }

    /**
     * Registers an iterable of Closeables, and returns that iterable.
     * @param iterable iterable of Closeables
     * @param <T> the Closeable type
     * @param <I> the iterable type
     * @return the iterable you passed in; lets you easily declare and register an object in one line
     */
    public <T extends Closeable,I extends Iterable<T>> I registerIterable(I iterable)
    {
        iterables.add(iterable);
        return iterable;
    }

    /**
     * <p>Tells {@link #close()} to throw this exception.</p>
     *
     * <p>You should use this if you're using the {@linkplain MultiCloser} in a <tt>finally</tt> clause, and the
     * <tt>try</tt> clause (or one of the <tt>catch</tt> clauses) is throwing an exception of its own. In that case,
     * you still want to close everything, but you want this throwable, which was the first throwable you caught,
     * to be what's finally thrown.</p>
     *
     * <p><strong>NOTE:</strong> You should only call this once per {@linkplain MultiCloser}. Calling it
     * multiple times will result in assertion being thrown, if assertions are enabled.</p>
     * @param throwable the Throwable to throw while closing
     */
    public void throwException(Throwable throwable)
    {
        if (throwable == null)
        {
            throw new NullPointerException("can't pass null arg to throwException()");
        }
        assert explicitThrow == null;
        explicitThrow = throwable;
    }

    /**
     * <p>Attempts to close every registered Closeable.</p>
     *
     * <p>If one or more Closeable's <tt>close()</tt> method throws an exception, this method will also throw
     * an exception according to these rules:</p>
     * <ol>
     *  <li>If at least one IOException was thrown, this method will throw an IOException whose cause is the first
     * thrown IOException.</li>
     *  <li>If one or more unchecked exceptions were thrown, but no IOExceptions were thrown, then this method will
     * throw a RuntimeException that wraps the first thrown Throwable</li>
     * </ol>
     * @throws IOException if at least one IOException was thrown
     * @throws RuntimeException if at least one Throwable was thrown, but no IOExceptions were thrown
     */
    @Override
    public void close() throws IOException
    {
        Throwable thrown = explicitThrow;
        int thrownCount = thrown == null ? 0 : 1;
        boolean ioExceptionCaught = false;
        List<Throwable> extras = new LinkedList<Throwable>();

        List<Closeable> localCloseables = new LinkedList<Closeable>(closeables);
        for (Iterable<? extends Closeable> iterator : iterables)
        {
            for(Closeable closeable : iterator)
            {
                localCloseables.add(closeable);
            }
        }

        for (Closeable closeable : localCloseables)
        {
            try
            {
                closeable.close();
            }
            catch (Throwable e)
            {
                extras.add(e);
                ++thrownCount;
                boolean firstIoException = false;
                if (explicitThrow == null)
                {
                    if (e instanceof IOException)
                    {
                        firstIoException = !ioExceptionCaught;
                        ioExceptionCaught = true;
                    }
                    if (thrown == null || firstIoException)
                    {
                        thrown = e;
                    }
                }
            }
        }

        if (thrown != null)
        {
            extras.remove(thrown);
        }
        if (printExtraThrowables && !extras.isEmpty())
        {
            int size = extras.size();
            System.err.println(size + (size == 1 ? " throwable was" : " throwables were") + " not thrown. Dumping:" );
            for (Enumerated<Throwable> throwable : EnumeratingIterator.of(extras))
            {
                System.err.println("********* EXTRA THROWABLE " + throwable.count() + " *****************************");
                if (thrown != null)
                {
                    System.err.println("********* for " + thrown.getClass() + ": " + thrown.getMessage());
                }
                //noinspection ThrowableResultOfMethodCallIgnored
                throwable.get().printStackTrace();
            }
        }

        if (thrownCount == 0)
        {
            return;
        }
        final String msg;
        if (thrownCount == 1)
        {
            msg = "one " + (ioExceptionCaught? "IOException" : "unchecked exception") + " caught";
        }
        else
        {
            msg = thrownCount + " items caught; first "
                    + (ioExceptionCaught? "IOException" : "unchecked exception")
                    + " is listed as this exception's cause";
        }
        if (ioExceptionCaught)
        {
            throw new IOException(msg, thrown);
        }
        throw new RuntimeException(msg, thrown);
    }

    void setPrintingOfExtraThrowables(boolean shouldPrint)
    {
        this.printExtraThrowables = shouldPrint;
    }
}
