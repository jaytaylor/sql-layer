/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.qp.physicaloperator.PhysicalOperator;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.SchemaAISBased;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.util.Strings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class OperatorStoreTest {

    @Test
    public void giUpdatePlan_C_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        PhysicalOperator plan = OperatorStore.groupIndexCreationPlan(
                schema,
                gi(ais, "gi_name"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
                "GroupScan_Default(shallow hkey-bound scan on GroupTable(sch._akiban_sch_customer -> sch.customer)NO_LIMIT)"
        );
        assertEquals("plan description", expected, plan.describePlan());
    }

    @Test
    public void giUpdatePlan_CI_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        PhysicalOperator plan = OperatorStore.groupIndexCreationPlan(
                schema,
                gi(ais, "gi_name_sku"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on GroupTable(sch._akiban_sch_customer -> sch.customer)NO_LIMIT)",
            "Flatten_HKeyOrdered(sch.item INNER sch.customer)",
            "Flatten_HKeyOrdered(flatten(sch.item, sch.customer) INNER sch.order)",
            "Flatten_HKeyOrdered(flatten(flatten(sch.item, sch.customer), sch.order) INNER sch.item)"
        );
        assertEquals("plan description", expected, plan.describePlan());
    }

    @Test
    public void giUpdatePlan_CI_fromI() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        PhysicalOperator plan = OperatorStore.groupIndexCreationPlan(
                schema,
                gi(ais, "gi_name_sku"),
                rowType(ais, schema, "item")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on GroupTable(sch._akiban_sch_customer -> sch.customer)NO_LIMIT)",
            "AncestorLookup_Default(sch.item -> [sch.customer, sch.order])",
            "Flatten_HKeyOrdered(sch.item INNER sch.customer)",
            "Flatten_HKeyOrdered(flatten(sch.item, sch.customer) INNER sch.order)",
            "Flatten_HKeyOrdered(flatten(flatten(sch.item, sch.customer), sch.order) INNER sch.item)"
        );
        assertEquals("plan description", expected, plan.describePlan());
    }

    // private static methods

    private static UserTableRowType rowType(AkibanInformationSchema ais, Schema schema, String tableName) {
        return schema.userTableRowType(ais.getUserTable(SCHEMA_NAME, tableName));
    }

    private static GroupIndex gi(AkibanInformationSchema ais, String indexName) {
        GroupIndex result = null;
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex possible : group.getIndexes()) {
                if (indexName.equals(possible.getIndexName().getName())) {
                    if (result != null) {
                        throw new RuntimeException("multiple GIs found for " + indexName);
                    }
                    result = possible;
                }
            }
        }
        if (result == null) {
            throw new RuntimeException("GI not found: " + indexName);
        }
        return result;
    }

    private static Schema schema(AkibanInformationSchema ais) {
        return new SchemaAISBased(ais);
    }

    private static AkibanInformationSchema coia() {
        return AISBBasedBuilder.create(SCHEMA_NAME)
                .userTable("customer")
                    .colLong("cid")
                    .colString("name", 32)
                    .colLong("priority")
                .userTable("order")
                    .colLong("oid")
                    .colLong("c_id")
                    .colLong("date")
                    .colString("description", 128)
                    .joinTo("customer").on("c_id", "cid")
                .userTable("item")
                    .colLong("iid")
                    .colLong("o_id")
                    .colLong("sku")
                    .colLong("quantity")
                    .joinTo("order").on("o_id", "oid")
                .userTable("address")
                    .colLong("aid")
                    .colLong("c_id")
                    .colString("street", 256)
                    .colString("state", 2)
                .groupIndex("gi_name").on("customer", "name")
                .groupIndex("gi_name_sku").on("customer", "name").and("item", "sku")
                .ais();
    }

    // consts

    private static final String SCHEMA_NAME = "sch";
}
