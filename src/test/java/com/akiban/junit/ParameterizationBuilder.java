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

package com.akiban.junit;

import java.util.LinkedList;
import java.util.List;

public final class ParameterizationBuilder
{
    private final List<Parameterization> list;

    /**
     * Creates a builder with no starting parameterizations.
     */
    public ParameterizationBuilder()
    {
        this(new LinkedList<Parameterization>());
    }

    /**
     * Creates a builder backed by a list. Changes to the one will be reflected in the other, and {@linkplain #asList()}
     * will return the same instance as is passed to this method.
     * @param backingList The list that backs this builder.
     */
    public ParameterizationBuilder(List<Parameterization> backingList)
    {
        list = backingList;
    }

    /**
     * Adds a parameterization to the end of the list.
     * @param name the parameterization's name
     * @param args the parameterization's arguments
     */
    public void add(String name, Object... args)
    {
        list.add(Parameterization.create(name, args));
    }

    /**
     * Adds a parameterization that's expected to fail to the end of the list
     * @param name the parameterization's name
     * @param args the parameterization's arguments
     */
    public void addFailing(String name, Object... args)
    {
        list.add(Parameterization.failing(name, args));
    }

    /**
     * Adds a parameterization to the end of the list.
     * @param name the parameterization's name
     * @param expectedToPass whether the parameterization is expected to pass any tests
     * @param args the parameterization's arguments
     */
    public void create(String name, boolean expectedToPass, Object... args)
    {
        list.add(new Parameterization(name, expectedToPass, args));
    }

    /**
     * <p>Performs a cartesian product of parameterizations by prepending each of the incoming args to each
     * existing parameter's arg list.</p>
     *
     * For instance, if you have parameters <tt>[ ("A", 1, 2, 3) and ("B", 4, 5, 6) ]</tt>, and you called
     * <tt>multiplyParametersByPrepending("foo-", 'c', "bar", 'd')</tt>, you would get the following
     * parameterizations, in this order:
     * <ol>
     *  <li>"foo-A", 'c', 1, 2, 3</li>
     *  <li>"bar-A", 'd', 1, 2, 3</li>
     *  <li>"foo-B", 'c', 4, 5, 6</li>
     *  <li>"bar-B", 'd', 1, 2, 3</li>
     * </ol>
     * @param args the list of named args to multiply by. Every other element (starting with the first) in the list must
     * be a String that represents the name prefix; the element following each prefix is the argument to append to
     * the args list.
     * @throws IllegalArgumentException if the args aren't given as String-Object pairings.
     * @throws IllegalStateException if there are no existing parameterizations to multiply by
     */
    public void multiplyParametersByPrepending(Object... args)
    {
        cartesianProduct(false, args);
    }

    /**
     * <p>Performs a cartesian product of parameterizations by appending each of the incoming args to each
     * existing parameter's arg list. If you supply a prefix, it will also be appended to each parameter's name.</p>
     *
     * <p>This works just like {@link #multiplyParametersByPrepending(Object...)}, only each parameter label and
     * value is appended instead of prepended.</p>
     * @param args the list of named args to multiply by. Every other element (starting with the first) in the list must
     * be a String that represents the name suffix; the element following each suffix is the argument to append to
     * the args list.
     * @throws IllegalArgumentException if the args aren't given as String-Object pairings.
     * @throws IllegalStateException if there are no existing parameterizations to multiply by
     * @see #multiplyParametersByPrepending(Object...)
     */
    public void multiplyParametersByAppending(Object... args)
    {
        cartesianProduct(true, args);
    }

    private void cartesianProduct(boolean append, Object[] args)
    {
        boolean usingAsserts = true;
        //noinspection AssertWithSideEffects
        assert (usingAsserts=true); // side effect is okay in this usage
        final Object PLACEHOLDER = usingAsserts ? new Object() : null;

        if (list.size() == 0)
        {
            throw new IllegalStateException("can't multiply yet -- no args defined");
        }
        if ( (args.length % 2) != 0)
        {
            throw new IllegalArgumentException("odd number of arguments");
        }

        List<Parameterization> product = new LinkedList<Parameterization>();
        for (Parameterization param : list)
        {
            Object[] origArgs = param.getArguments();
            Object[] newArgs = new Object[origArgs.length + 1];
            if (usingAsserts)
            {
                for (int i=0; i < newArgs.length; ++i)
                {
                    newArgs[i] = PLACEHOLDER;
                }
            }
            System.arraycopy(origArgs, 0, newArgs, append ? 0 : 1, origArgs.length);

            for(int i=0; i < args.length; i+= 2)
            {
                String label;
                try
                {
                    label = (String)args[i];
                }
                catch (ClassCastException e)
                {
                    throw new IllegalArgumentException("argument at index " + i + " is not a String");
                }

                if (label == null)
                {
                    label = param.getName();
                }
                else
                {
                    label = append ?
                            param.getName() + label
                            : label + param.getName();
                }
                newArgs[append ? origArgs.length : 0] = args[i+1];
                if (usingAsserts)
                {
                    for (Object newArg : newArgs)
                        assert newArg != PLACEHOLDER;
                }
                product.add(new Parameterization(label, param.expectedToPass(), newArgs));
            }
        }

        list.clear();
        list.addAll(product);
    }

    /**
     * Represents this builder's parameterizations. The returning list is backed by this builder, so changes
     * to either are seen in both.
     * @return the list that backs this builder
     */
    public List<Parameterization> asList()
    {
        return list;
    }
}
