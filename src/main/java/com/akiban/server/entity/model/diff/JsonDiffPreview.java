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

import com.akiban.server.entity.changes.AttributeLookups;
import com.akiban.server.entity.changes.SpaceModificationHandler;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;
import java.io.IOException;
import java.io.StringWriter;
import java.util.UUID;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * 
 * Generate change-log in json
 */
public class JsonDiffPreview implements SpaceModificationHandler
{
    private static final JsonFactory factory = new JsonFactory();
    private static final boolean useDefaultPrettyPrinter = true;
    
    private final JsonGenerator jsonGen;
    private final StringWriter stringWriter;
    
    public JsonDiffPreview()
    {
        stringWriter = new StringWriter();
        try
        {
            jsonGen = factory.createJsonGenerator(stringWriter);
            if (useDefaultPrettyPrinter)
                jsonGen.useDefaultPrettyPrinter();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    public StringWriter toJSON()
    {
        try
        {
            jsonGen.flush();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        return stringWriter;
    }

    @Override
    public void beginEntity(UUID entityUUID) {
        // None
    }

    @Override
    public void addEntity(UUID entityUuid)
    {
        try
        {
            startObject();
            entry("action", "add_entity");
            entry("destructive", false);
            entry("uuid", entityUuid.toString());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void dropEntity(Entity dropped, String oldName)
    {
        try
        {
            startObject();
            entry("action", "drop_entity");
            entry("destructive", true);
            entry("uuid", dropped.uuid());
            entry("name", oldName);
            entry("index_definition", dropped.getIndexes());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void renameEntity(UUID entityUuid, String oldName)
    {
        try
        {
            startObject();
            entry("action", "rename_entity");
            entry("destructive", false);
            entry("uuid", entityUuid);
            entry("old_name", oldName);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void beginAttributes(AttributeLookups oldLookups, AttributeLookups newLookups) {
        // None
    }

    @Override
    public void addAttribute(UUID attributeUuid)
    {
        try
        {
            startObject();
            entry("action", "add_attribute");
            entry("destructive", false);
            entry("uuid", attributeUuid);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void dropAttribute(Attribute dropped)
    {
        try
        {
            startObject();
            entry("action", "drop_attribute");
            entry("destructive", true);
            entry("uuid", dropped.getUUID());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName)
    {
        try
        {
            startObject();
            entry("action", "rename_attribute");
            entry("destructive", false);
            entry("uuid", attributeUuid);
            entry("old_name", oldName);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void changeScalarType(UUID scalarUuid, Attribute afterChange)
    {
        try
        {
            startObject();
            entry("action", "change_scalar_type");
            entry("destructive", true);
            entry("uuid", scalarUuid);
            entry("new_scalar_type", afterChange.getType());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange)
    {
        try
        {
            startObject();
            entry("action", "change_scalar_validations");
            entry("destructive", true);
            entry("uuid", scalarUuid);
            entry("new_validations", afterChange.getValidation());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange)
    {
        try
        {
            startObject();
            entry("action", "change_scalar_properties");
            entry("destructive", true);
            entry("uuid", scalarUuid);
            entry("new_properties", afterChange.getProperties());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void endAttributes() {
        // None
    }

    @Override
    public void addEntityValidation(Validation validation)
    {
        try
        {
            startObject();
            entry("action", "add_entity_validation");
            entry("destructive", false);
            entry("new_validation", validation);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void dropEntityValidation(Validation validation)
    {   
        try
        {
            startObject();
            entry("action", "drop_entity+validation");
            entry("destructive", true);
            entry("dropped_validation", validation);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void addIndex(String name)
    {
        try
        {
            startObject();
            entry("action", "add_index");
            entry("destructive", false);
            entry("new_index", name);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void dropIndex(String name, EntityIndex index)
    {
        try
        {
            startObject();
            entry("action", "drop_index");
            entry("destructive", true);
            entry("name", name);
            entry("index", index);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void renameIndex(EntityIndex index, String oldName, String newName)
    {
        try
        {
            startObject();
            entry("action", "rename_index");
            entry("destructive", false);
            entry("old_name", oldName);
            entry("new_name", newName);
            entry("index", index);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void endEntity() {
        // None
    }

    @Override
    public void error(String message)
    {
        try
        {
            startObject();
            entry("error", message);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
            
        }
    }
    
    private void startObject() throws IOException
    {
        jsonGen.writeStartObject();
    }
    
    private void endObject() throws IOException
    {
        jsonGen.writeEndObject();
    }
    
    private void entry(String name, UUID uuid) throws IOException
    {
        entry(name, uuid.toString());
    }

    private void entry(String name, Object value) throws IOException
    {
        jsonGen.writeFieldName(name);
        jsonGen.writeObject(value);
    }
}
