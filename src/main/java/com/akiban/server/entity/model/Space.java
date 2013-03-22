
package com.akiban.server.entity.model;

import com.google.common.base.Function;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonMappingException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static com.akiban.util.JsonUtils.createJsonGenerator;
import static com.akiban.util.JsonUtils.readValue;

public final class Space {

    public static Space create(Reader reader, Function<String, UUID> uuidGenerator) throws IOException {
        Space result;
        try {
            result = readValue(reader, Space.class);
        } catch (JsonMappingException e) {
            if (e.getCause() instanceof IllegalEntityDefinition)
                throw (IllegalEntityDefinition) e.getCause();
            throw e;
        }
        result.visit(new Validator(uuidGenerator));
        return result;
    }

    public static Space readSpace(String fileName, Class<?> forClass, Function<String, UUID> uuidGenerator) {
        try (InputStream is = forClass.getResourceAsStream(fileName)) {
            if (is == null) {
                throw new RuntimeException("resource not found: " + fileName);
            }
            Reader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
            return create(reader, uuidGenerator);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <E extends Exception> void visit(EntityVisitor<E> visitor) throws E {
        for (Map.Entry<String, Entity> entry : entities.entrySet()) {
            entry.getValue().accept(entry.getKey(), visitor);
        }
    }

    public Map<String, Entity> getEntities() {
        return entities;
    }

    void setEntities(Map<String, Entity> entities) {
        this.entities = Collections.unmodifiableMap(new TreeMap<>(entities));
    }

    @Override
    public String toString() {
        return entities.toString();
    }

    public static Space create(Map<String, Entity> entities, Function<String, UUID> uuidGenerator) {
        Space space = new Space();
        space.setEntities(entities);
        space.visit(new Validator(uuidGenerator));
        return space;
    }

    public void generateJson(JsonGenerator json) throws IOException {
        json.writeStartObject();
        if (!entities.isEmpty()) {
            json.writeObjectFieldStart("entities");
            visit(new JsonEntityFormatter(json));
            json.writeEndObject();
        }
        json.writeEndObject();
    }

    public String toJson() {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = createJsonGenerator(writer);
            generateJson(generator);
            generator.flush();
            writer.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    Space() {}

    private Map<String, Entity> entities = Collections.emptyMap();

    private static class Validator extends AbstractEntityVisitor {

        @Override
        public void visitEntity(String name, Entity entity) {
            if (entity.uuid() == null)
                entity.setUuid(uuidGenerator.apply(name));
            validateUUID(entity.uuid());
            ensureUnique(name);
            if (entity.getAttributes() == null || entity.getAttributes().isEmpty())
                throw new IllegalEntityDefinition("no attributes set for entity: " + name);
        }

        @Override
        public void visitScalar(String name, Attribute scalar) {
            if (scalar.getUUID() == null)
                scalar.setUuid(uuidGenerator.apply(name));
            validateUUID(scalar.getUUID());
            if (scalar.getType() == null)
                throw new IllegalEntityDefinition("no type set for scalar");
            if (scalar.getAttributes() != null)
                throw new IllegalEntityDefinition("attributes can't be set for scalar");
        }

        @Override
        public void visitCollection(String name, Attribute collection) {
            if (collection.getUUID() == null)
                collection.setUuid(uuidGenerator.apply(name));
            validateUUID(collection.getUUID());
            ensureUnique(name);
            if (collection.getType() != null)
                throw new IllegalEntityDefinition("type can't be set for collection");
            if (collection.getAttributes() == null || collection.getAttributes().isEmpty())
                throw new IllegalEntityDefinition("no attributes set for collection");
        }

        private void validateUUID(UUID uuid) {
            if (uuid == null)
                throw new IllegalEntityDefinition("no uuid specified");
            if (!uuids.add(uuid))
                throw new IllegalEntityDefinition("duplicate uuid found: " + uuid);
        }

        private void ensureUnique(String name) {
            if (!collectionNames.add(name))
                throw new IllegalEntityDefinition("duplicate name within entity and collections: " + name);
        }

        private Validator(Function<String, UUID> uuidGenerator) {
            this.uuidGenerator = uuidGenerator == null ? requireUUIDs : uuidGenerator;
        }

        private final Set<UUID> uuids = new HashSet<>();
        private final Set<String> collectionNames = new HashSet<>();
        private final Function<String, UUID> uuidGenerator;
    }


    public static final Function<String, UUID> randomUUIDs = new Function<String, UUID>() {
        @Override
        public UUID apply(String input) {
            return UUID.randomUUID();
        }
    };

    public static final Function<String, UUID> requireUUIDs = new Function<String, UUID>() {
        @Override
        public UUID apply(String input) {
            return null;
        }
    };
}
