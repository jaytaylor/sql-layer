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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class Parameterization
{
    private String name;
    private final List<Object> args;
    private boolean expectedToPass;

    public static Parameterization create(String name, Object... args)
    {
        return new Parameterization(name, true, args);
    }

    public static Parameterization failing(String name, Object... args)
    {
        return new Parameterization(name, false, args);
    }

    public Parameterization(String name, boolean isPassing, Object... args)
    {
        if (name == null) {
            throw new IllegalArgumentException("name can't be null");
        }
        if (args == null) {
            throw new IllegalArgumentException("args can't be null");
        }
        this.name = name;
        this.args = new LinkedList<Object>(Arrays.asList(args));
        this.expectedToPass = isPassing;
    }

    /**
     * <p>Creates a copy of this parameterization.</p>
     *
     * <p>The original parameterization's arguments list is copied shallowly.</p>
     * @param copyFrom the original parameterization
     */
    public Parameterization(Parameterization copyFrom)
    {
        this.name = copyFrom.name;
        this.args = new LinkedList<Object>(copyFrom.args);
        this.expectedToPass = copyFrom.expectedToPass;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Returns this parameterization's arguments as an Object array.
     *
     * This array is <em>not</em> backed by the parameterization, so changes in one are not reflected in the other.
     * However, the array is a shallow copy, so modifying an element of the parameterization in the list will be
     * reflected in the array, and the vice versa.
     * @return the arguments
     */
    public Object[] getArguments()
    {
        return args.toArray();
    }

    /**
     * Returns this parameterization's arguments as a list.
     *
     * This list is backed by the parameterization, so changes to one are reflected in the other.
     * @return a list of arguments, backed by the parameterization
     */
    public List<Object> getArgsAsList()
    {
        return args;
    }

    public boolean expectedToPass()
    {
        return expectedToPass;
    }

    public void setExpectedToPass(boolean expectedToPass)
    {
        this.expectedToPass = expectedToPass;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        Parameterization that = (Parameterization) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    public boolean equivalent(Parameterization other)
    {
        if (!this.name.equals(other.name))
            return false;
        if (this.expectedToPass != other.expectedToPass)
            return false;
        if (this.args.size() != other.args.size())
            return false;

        Iterator<Object> myIter = this.args.iterator();
        Iterator<Object> otherIter = other.args.iterator();

        while (myIter.hasNext())
        {
            Object mine = myIter.next();
            Object his = otherIter.next();

            if (mine == null)
            {
                if (his != null)
                {
                    return false;
                }
            }
            else if (!mine.equals(his))
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "Parameterization[" + (expectedToPass? "PASSING " : "FAILING ") + name + ": " + args + " ]";
    }
}
