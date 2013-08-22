/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.entity.model;

public abstract class AbstractEntityVisitor implements EntityVisitor {
    @Override
    public void enterTopEntity(Entity entity) {
        visitEntityInternal(entity);
    }

    @Override
    public void leaveTopEntity() {
        leaveEntity();
    }

    @Override
    public void enterCollections() {
    }

    @Override
    public void enterCollection(EntityCollection collection) {
        visitEntityInternal(collection);
    }

    @Override
    public void leaveCollection() {
        leaveEntity();
    }

    @Override
    public void leaveCollections() {
    }

    /**
     * Visits any entity, regardless of whether it's top-level or a collection.
     * This is invoked from {@linkplain #enterTopEntity} and {@linkplain #enterCollection}, so if you use this
     * method and override either of those, make sure to invoke <tt>super</tt> within the override.
     * @param entity the entity to visit
     */
    protected void visitEntity(Entity entity) {
    }

    /**
     * Visits any EntityElement: entities, collections and fields.
     * This is invoked from {@linkplain #enterTopEntity} and {@linkplain #enterCollection}, so if you use this
     * method and override either of those, make sure to invoke <tt>super</tt> within the override.
     * @param element the entity to visit
     */
    protected void visitEntityElement(EntityElement element) {
    }

    /**
     * Leaves any entity, regardless of whether it's top-level or a collection.
     * This is invoked from {@linkplain #leaveTopEntity} and {@linkplain #leaveCollection}, so if you use this
     * method and override either of those, make sure to invoke <tt>super</tt> within the override.
     */
    protected void leaveEntity() {
    }

    protected void visitEntityField(EntityField field) {
    }

    /**
     * Default visiting of an entity. We could have also just had this be the default implementation of
     * {@linkplain #visitEntity}, but then the API gets a bit more complicated in terms of which methods you can
     * and can't override (without calling super).
     * @param entity
     */
    private void visitEntityInternal(Entity entity) {
        visitEntityElement(entity);
        visitEntity(entity);
        for (EntityField field : entity.getFields()) {
            visitEntityElement(field);
            visitEntityField(field);
        }
    }
}
