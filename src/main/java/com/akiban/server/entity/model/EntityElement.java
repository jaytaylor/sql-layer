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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


import java.util.Comparator;
import java.util.UUID;

public abstract class EntityElement {

    public UUID getUuid() {
        return uuid;
    }

    @JsonProperty("uuid")
    public void setUuid(String string) {
        UUID uuid;
        try {
            uuid = string == null ? null : UUID.fromString(string);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalEntityDefinition("invalid uuid");
        }
        setUuid(uuid);
    }

    @JsonIgnore
    void setUuid(UUID uuid) {
        if (this.uuid != null)
            throw new IllegalStateException("uuid already set: " + this.uuid);
        this.uuid = uuid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    private UUID uuid;
    private String name;

    public static final Comparator<? super EntityElement> byName = new Comparator<EntityElement>() {
        @Override
        public int compare(EntityElement o1, EntityElement o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };

    public static final Function<? super EntityElement, String> toName = new Function<EntityElement, String>() {
        @Override
        public String apply(EntityElement input) {
            return input == null ? null : input.getName();
        }
    };

    public static final Function<? super EntityElement, ? extends UUID> toUuid = new Function<EntityElement, UUID>() {
        @Override
        public UUID apply(EntityElement input) {
            return input == null ? null : input.getUuid();
        }
    };

    public static boolean sameByUuid(EntityElement one, EntityElement two) {
        if (one == null)
            return two == null;
        return two != null && one.getUuid().equals(Preconditions.checkNotNull(two.getUuid()));
    }
}
