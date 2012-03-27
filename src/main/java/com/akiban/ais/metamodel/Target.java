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

package com.akiban.ais.metamodel;

import java.util.Map;

public abstract class Target implements ModelNames
{
    public abstract void deleteAll();

    public abstract void writeCount(int count);

    public abstract void writeVersion(int modelVersion); 
    
    public void writeType(Map<String, Object> map) 
    {
        write(type, map);
    }

    public final void writeGroup(Map<String, Object> map) 
    {
        write(group, map);
    }

    public final void writeTable(Map<String, Object> map) 
    {
        write(table, map);
    }

    public final void writeColumn(Map<String, Object> map) 
    {
        write(column, map);
    }

    public final void writeJoin(Map<String, Object> map) 
    {
        write(join, map);
    }

    public final void writeJoinColumn(Map<String, Object> map) 
    {
        write(joinColumn, map);
    }

    public final void writeIndex(Map<String, Object> map) 
    {
        write(index, map);
    }

    public final void writeIndexColumn(Map<String, Object> map) 
    {
        write(indexColumn, map);
    }

    protected abstract void write(final String string, final Map<String, Object> map);

    public abstract void close();
}