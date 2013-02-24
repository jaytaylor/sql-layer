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

package com.akiban.server.entity.model;

import com.akiban.util.JUnitUtils;
import com.google.common.collect.BiMap;

import java.util.Map;
import java.util.Set;

class ToStringVisitor extends JUnitUtils.MessageTaker implements EntityVisitor<RuntimeException> {

    @Override
    public void visitEntity(String name, Entity entity) {
        message("visiting entity", name, entity);
    }

    @Override
    public void leaveEntity() {
        message("leaving entity");
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
        message("visiting scalar", name, scalar);
    }

    @Override
    public void visitCollection(String name, Attribute collection) {
        message("visiting collection", name, collection);
    }

    @Override
    public void leaveCollection() {
        message("leaving collection");
    }

    @Override
    public void leaveEntityAttributes() {
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) {
        for (Map.Entry<String, EntityIndex> index : indexes.entrySet())
            message("visiting index", index.getKey(), index.getValue());
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) {
        for (Validation validation : validations)
            message("visiting entity validation", validation);

    }

}
