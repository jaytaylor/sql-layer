
package com.akiban.server.entity.fromais;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.Space;
import com.google.common.base.Function;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class AisToSpace {

    public static Space create(AkibanInformationSchema ais, Function<String, UUID> uuidGenerator) {
        Map<String, Entity> entities = new HashMap<>();
        for (Group group : ais.getGroups().values()) {
            UserTable root = group.getRoot();
            String entityName = root.getName().getTableName();
            if (entities.containsKey(entityName))
                throw new InconvertibleAisException("duplicate table name: " + entityName);
            Entity entity = new EntityBuilder(root).getEntity();
            entities.put(entityName, entity);
        }
        return Space.create(entities, uuidGenerator);
    }

    private AisToSpace() {}
}
