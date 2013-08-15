/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.entity.model.diff;

import com.foundationdb.server.entity.changes.AbstractSpaceModificationHandler;
import com.foundationdb.server.entity.changes.SpaceModificationHandler;
import com.foundationdb.server.entity.model.Entity;
import com.foundationdb.server.entity.model.EntityField;
import com.foundationdb.server.entity.model.EntityIndex;
import com.foundationdb.server.entity.model.Validation;
import java.io.IOException;
import java.io.Writer;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonGenerator;

import static com.foundationdb.util.JsonUtils.createJsonGenerator;

/**
 * 
 * Generate change-log in json
 */
public class JsonDiffPreview extends AbstractSpaceModificationHandler
{
    private final JsonGenerator jsonGen;
    private final Writer writer;
    private final Set<Entity> modifiedEntities;
    private Entity oldEntity;
    private Entity updatedEntity;
    private Entity updatedTopLevelEntity;
    private int level;
    private boolean finished = false;

    private static final Comparator<Entity> entitiesByName = new Comparator<Entity>() {
        @Override
        public int compare(Entity o1, Entity o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };

    public JsonDiffPreview(Writer writer)
    {
        this.writer = writer;
        try
        {
            jsonGen = createJsonGenerator(writer);
            jsonGen.writeStartArray();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        this.modifiedEntities = new TreeSet<>(entitiesByName);
    }

    public void describeModifiedEntities() {
        try
        {
            startObject();
            jsonGen.writeArrayFieldStart("modified_entities");
            for (Entity entity : modifiedEntities)
                jsonGen.writeObject(entity);
            jsonGen.writeEndArray();
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    public void finish() {
        if(!finished) {
            try
            {
                jsonGen.writeEndArray();
                jsonGen.flush();
                writer.flush();
            }
            catch (IOException ex)
            {
                throw new DiffIOException(ex);
            }
            finished = true;
        }
    }

    @Override
    public void beginEntity(Entity oldEntity, Entity newEntity) {
        if (level++ == 0)
            this.updatedTopLevelEntity = newEntity;
        this.oldEntity = oldEntity;
        this.updatedEntity = newEntity;
    }

    @Override
    public void addEntity(Entity entity)
    {
        try
        {
            startObject();
            entry("action", "add_entity");
            entry("destructive", false);
            entry("uuid", entity.getUuid().toString());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        if (level == 0)
            entityModified(entity);
    }

    @Override
    public void dropEntity(Entity dropped)
    {
        try
        {
            startObject();
            entry("action", "drop_entity");
            entry("destructive", true);
            entry("uuid", dropped.getUuid());
            entry("name", dropped.getName());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
    }

    @Override
    public void renameEntity()
    {
        try
        {
            startObject();
            entry("action", "rename_entity");
            entry("destructive", false);
            entry("uuid", oldEntity.getUuid());
            entry("old_name", oldEntity.getName());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified(updatedTopLevelEntity);
    }

    @Override
    public void addField(UUID attributeUuid)
    {
        try
        {
            startObject();
            entry("action", "add_field");
            entry("destructive", false);
            entry("uuid", attributeUuid);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void dropField(UUID dropped)
    {
        try
        {
            startObject();
            entry("action", "drop_field");
            entry("destructive", true);
            entry("uuid", dropped);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void fieldOrderChanged(UUID uuid) {
        try
        {
            startObject();
            entry("action", "field_order_changed");
            entry("uuid", uuid.toString());
            entry("destructive", false);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void renameField(UUID uuid)
    {
        try
        {
            startObject();
            entry("action", "rename_field");
            entry("destructive", false);
            entry("uuid", uuid);
            entry("old_name", lookupField(oldEntity, uuid).getName());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void changeFieldType(UUID uuid)
    {
        try
        {
            startObject();
            entry("action", "change_field_type");
            entry("destructive", true);
            entry("uuid", uuid);
            entry("new_field_type", lookupField(updatedEntity, uuid).getType());
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void changeFieldValidations(UUID field)
    {
        try
        {
            startObject();
            entry("action", "change_field_validations");
            entry("destructive", true);
            entry("uuid", field);
            jsonGen.writeArrayFieldStart("new_validations");
            for (Validation v : lookupField(updatedEntity, field).getValidations()) {
                jsonGen.writeStartObject();
                jsonGen.writeObjectField(v.getName(), v.getValue());
                jsonGen.writeEndObject();
            }
            jsonGen.writeEndArray();
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void changeFieldProperties(UUID field)
    {
        try
        {
            startObject();
            entry("action", "change_field_properties");
            entry("destructive", true);
            entry("uuid", field);
            jsonGen.writeObjectFieldStart("new_properties");
            for (Map.Entry<String, Object> prop : lookupField(updatedEntity, field).getProperties().entrySet())
                jsonGen.writeObjectField(prop.getKey(), prop.getValue());
            jsonGen.writeEndObject();
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
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
        entityModified();
    }

    @Override
    public void dropIndex(String name)
    {
        try
        {
            startObject();
            entry("action", "drop_index");
            entry("destructive", true);
            entry("name", name);
            endObject();
        }
        catch (IOException ex)
        {
            throw new DiffIOException(ex);
        }
        entityModified();
    }

    @Override
    public void endEntity() {
        --level;
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
        try {
            jsonGen.writeObject(value);
        } catch(IllegalStateException e) {
            // writeObject throws if it doesn't know how to serialize the type.
            // This isn't ideal but without repeating a bunch of instanceofs, no other way to know success.
            jsonGen.writeObject(value.toString());
        }
    }

    private static EntityField lookupField(Entity oldEntity, UUID uuid) {
        for (EntityField field : oldEntity.getFields()) {
            if (field.getUuid().equals(uuid))
                return field;
        }
        throw new DiffIOException("no field found with UUID " + uuid);
    }

    private void entityModified() {
        assert updatedTopLevelEntity != null;
        modifiedEntities.add(updatedTopLevelEntity);
    }

    private void entityModified(Entity entity) {
        modifiedEntities.add(entity);
    }
}
