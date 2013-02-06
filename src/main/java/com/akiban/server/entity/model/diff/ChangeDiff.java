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
public class ChangeDiff implements SpaceModificationHandler
{
    private final JsonFactory factory = new JsonFactory();
    
    private /*final*/ JsonGenerator jsonGen; // find json print stream
    private /*final*/ StringWriter stringWriter;
    private boolean useDefaultPrettyPrinter = true;
    
    public ChangeDiff()
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
            ex.printStackTrace();
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
            ex.printStackTrace();
            //TOD:  REPORT error?
        }
        return stringWriter;
    }

    @Override 
    public void addEntry(UUID entityUuid)
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
            ex.printStackTrace();
            // TODO REPORT error back to client ?
        }
    }

    @Override
    public void dropEntry(Entity dropped)
    {
        try
        {
            startObject();
            entry("action", "drop_entity");
            entry("destructive", true);
            entry("uuid", dropped.uuid());
            entry("index_definition", dropped.getIndexes());
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            //TODO: report error?
        }
    }

    @Override
    public void renameEntry(UUID entityUuid, String oldName)
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
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void addAttribute(UUID attributeUuid)
    {
        //TODO: shouldn't there be two other arguments
        // being "field" and "value" (field : value)
        try
        {
            startObject();
            entry("action", "add_attribute");
            entry("destructive", false);
            entry("uuid", attributeUuid);
//            entry("field", field);
//            entry("value", value);
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
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
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName)
    {
        //TODO: need a third argument being new_name?
        try
        {
            startObject();
            entry("action", "rename_attribute");
            entry("destructive", false);
            entry("uuid", attributeUuid);
            entry("old_name", oldName);
            //entry("new_name", newName);
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
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
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange)
    {
        try
        {
            startObject();
            entry("action", "change_scalar_validations");
            entry("destructive", true); //TODO: or false?
            entry("uuid", scalarUuid);
            entry("new_validations", afterChange.getValidation());
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange)
    {
        try
        {
            startObject();
            entry("action", "change_scalar_properties");
            entry("destructive", true); // TODO: or false?
            entry("uuid", scalarUuid);
            entry("new_properties", afterChange.getProperties());
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void addEntityValidation(Validation validation)
    {
        //TODO: shouldn't there be an "uuid" argument,
        //      identifying what entity to add this validation to?
        try
        {
            startObject();
            entry("action", "add_entity_validation");
            entry("destructive", false);
            //entry("uuid", entityUuid);
            entry("new_validation", validation);
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void dropEntityValidation(Validation validation)
    {
        //TODO: shouldnt there be an "uuid" argument,
        //      identifying whose validation to drop?
        
        try
        {
            startObject();
            entry("action", "drop_entity+validation");
            entry("destructive", true);
            //entry("uuid", entityUuid);
            entry("dropped_validation", validation);
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void addIndex(EntityIndex index)
    {
        try
        {
            startObject();
            entry("action", "add_index");
            entry("destructive", false);
            entry("new_index", index);
            endObject();
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void dropIndex(String name, EntityIndex index)
    {
        //TODO: Whose 'name' is this?
        // name of the index? Or name of the entity/space where the index belongs?

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
            ex.printStackTrace();
            // TODO: report error
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
            ex.printStackTrace();
            // TODO: report error
        }
    }

    @Override
    public void error(String message)
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    private void startObject() throws IOException
    {
        //TODO: start locking the shared buffer ?
        jsonGen.writeStartObject();
    }
    
    private void endObject() throws IOException
    {
        jsonGen.writeEndObject();;
        //TODO: release the buffer ?
    }
    
    private void entry(String name, UUID uuid) throws IOException
    {
        // JSonGenerator.writeObject does not handle object of types UUID
        // So use it's String representation instead (for now)
        entry(name, uuid.toString());
    }

    private void entry(String name, Object value) throws IOException
    {
        jsonGen.writeFieldName(name);
        jsonGen.writeObject(value);
    }
}
