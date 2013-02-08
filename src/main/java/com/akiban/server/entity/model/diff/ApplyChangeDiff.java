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

package com.akiban.server.entity.model.diff;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.entity.changes.SpaceModificationHandler;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;
import java.io.StringWriter;
import java.util.UUID;

public class ApplyChangeDiff implements SpaceModificationHandler
{
    private AkibanInformationSchema ais;
    // TODO: Is it still neccessary to generate the JSON rep?

    @Override
    public void addEntity(UUID entityUuid)
    {
        
        throw new UnsupportedOperationException("Not supported yet.");
        // compile DDL statements?
    }

    @Override
    public void dropEntity(Entity dropped)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void renameEntity(UUID entityUuid, String oldName)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addAttribute(UUID attributeUuid)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void dropAttribute(Attribute dropped)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void changeScalarType(UUID scalarUuid, Attribute afterChange)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addEntityValidation(Validation validation)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void dropEntityValidation(Validation validation)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addIndex(String ame)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void dropIndex(String name, EntityIndex index)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void renameIndex(EntityIndex index, String oldName, String newName)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void error(String message)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
