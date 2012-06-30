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

package com.akiban.ais.model;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

public class Type
{
    @Override
    public String toString()
    {
        return "Type(" + name + ")";
    }

    public String name()
    {
        return name;
    }

    public Integer nTypeParameters()
    {
        return typeParameters;
    }

    public Boolean fixedSize()
    {
        return fixedSize;
    }

    public Long maxSizeBytes()
    {
        return maxStorageSizeBytes;
    }

    public String encoding()
    {
        return encoding;
    }

    public AkType akType() {
        return akType;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Type)) {
            return false;
        }
        Type type = (Type) object;
        return name.equals(type.name())
               && typeParameters.equals(type.nTypeParameters())
               && fixedSize.equals(type.fixedSize())
               && maxStorageSizeBytes.equals(type.maxSizeBytes())
               && encoding.equals(type.encoding());
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((typeParameters == null) ? 0 : typeParameters.hashCode());
        result = prime * result + ((fixedSize == null) ? 0 : fixedSize.hashCode());
        result = prime * result + ((maxStorageSizeBytes == null) ? 0 : maxStorageSizeBytes.hashCode());
        result = prime * result + ((encoding == null) ? 0 : encoding.hashCode());
        return result;
    }
    
    public Type(String name, Integer typeParameters, Boolean fixedSize, Long maxStorageSizeBytes, String encoding,
                AkType akType)
    {
        this.name = name;
        this.typeParameters = typeParameters;
        this.fixedSize = fixedSize;
        this.maxStorageSizeBytes = maxStorageSizeBytes;
        this.encoding = encoding;
        this.akType = akType;
    }

    public static Type create(AkibanInformationSchema ais, String name, Integer typeParameters, Boolean fixedSize, 
                              Long maxStorageSizeBytes, String encoding, AkType akType, TInstance instance) {
        Type type = new Type(name, typeParameters, fixedSize, maxStorageSizeBytes, encoding, akType);
        ais.addType(type);
        return type;
    }

    private final String name;
    private final Integer typeParameters;
    private final Boolean fixedSize;
    private final Long maxStorageSizeBytes;
    private final String encoding;
    private final AkType akType;
}
