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

package com.akiban.server.entity.fromais;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.Validation;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class EntityBuilder {

    public EntityBuilder(UserTable rootTable) {
        entity = Entity.modifiableEntity(uuidOrCreate(rootTable));
        buildScalars(entity.getAttributes(), rootTable);
        buildCollections(entity.getAttributes(), rootTable);
    }

    private void buildScalars(Map<String, Attribute> attributes, UserTable table) {
        PrimaryKey primaryKey = table.getPrimaryKey();
        List<Column> pkColumns = primaryKey == null ? null : primaryKey.getColumns();
        Join parentJoin = table.getParentJoin();
        Set<Column> parentJoinColumns;
        if (parentJoin == null) {
            parentJoinColumns = Collections.emptySet();
        }
        else {
            List<JoinColumn> joinColumns = parentJoin.getJoinColumns();
            parentJoinColumns = new HashSet<>(joinColumns.size());
            for (JoinColumn joinColumn : joinColumns)
                parentJoinColumns.add(joinColumn.getChild());
        }

        for (Column column : table.getColumns()) {
            if (parentJoinColumns.contains(column))
                continue;
            TInstance tInstance = column.tInstance();
            TClass tClass = tInstance.typeClass();
            String type = tClass.name().unqualifiedName();

            Attribute scalar = Attribute.modifiableScalar(uuidOrCreate(column), type);

            if (pkColumns != null) {
                int pkPos = pkColumns.indexOf(column);
                if (pkPos >= 0)
                    scalar.setSpinalPos(pkPos);
            }
            Map<String, Object> properties = scalar.getProperties();
            Collection<Validation> validations = scalar.getValidation();
            if (!tInstance.nullability())
                validations.add(new Validation("required", Boolean.TRUE));
            for (int classAttr = 0, max = tClass.nAttributes(); classAttr < max; ++classAttr) {
                String attrName = tClass.attributeName(classAttr);
                Object attrValue = tInstance.attributeToObject(classAttr);
                if (tClass.attributeIsPhysical(classAttr))
                    properties.put(attrName, attrValue);
                else
                    validations.add(new Validation(attrName, attrValue));
            }
            attributes.put(column.getName(), scalar);
        }
    }

    private void buildCollections(Map<String, Attribute> attributes, UserTable table) {
        List<Join> childJoins = table.getChildJoins();
        for (Join childJoin : childJoins) {
            UserTable child = childJoin.getChild();
            String childName = child.getName().getTableName();
            Attribute collection = buildCollection(child);
            addAttribute(attributes, childName, collection);
        }
    }

    private Attribute buildCollection(UserTable table) {
        Attribute collection = Attribute.modifiableCollection(uuidOrCreate(table));
        buildScalars(collection.getAttributes(), table);
        buildCollections(collection.getAttributes(), table);
        return collection;
    }

    private void addAttribute(Map<String, Attribute> attributes, String name, Attribute attribute) {
        if (attributes.containsKey(name))
            throw new InconvertibleAisException("duplicate attribute name: " + name);
        attributes.put(name, attribute);
    }

    public Entity getEntity() {
        return entity;
    }

    private static UUID uuidOrCreate(UserTable table) {
        UUID uuid = table.getUuid();
        assert uuid != null : table;
        return uuid;
    }

    private UUID uuidOrCreate(Column column) {
        UUID uuid = column.getUuid();
        assert uuid != null : column;
        return uuid;
    }

    private final Entity entity;
}
