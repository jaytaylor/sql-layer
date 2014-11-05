/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.junit;

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

        List<Parameterization> product = new LinkedList<>();
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
