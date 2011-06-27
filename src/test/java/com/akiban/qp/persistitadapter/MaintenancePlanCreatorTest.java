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
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.SchemaAISBased;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.SchemaFactory;
import com.akiban.util.Strings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public final class MaintenancePlanCreatorTest {

    @Test
    public void giUpdatePlan_C_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_name"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
                "GroupScan_Default(shallow hkey-bound scan on _akiban_sch_customer NO_LIMIT)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_CI_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_name_sku"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_CI_fromI() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_name_sku"),
                rowType(ais, schema, "item")
        );
        String expected = Strings.join(
            "GroupScan_Default(shallow hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.item -> [sch.customer, sch.order])",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_OCI_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_date_name_sku"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_OCI_fromO() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_date_name_sku"),
                rowType(ais, schema, "order")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.order -> [sch.customer])",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_OCI_fromI() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_date_name_sku"),
                rowType(ais, schema, "item")
        );
        String expected = Strings.join(
            "GroupScan_Default(shallow hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.item -> [sch.customer, sch.order])",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_OI_fromI() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_sku_date"),
                rowType(ais, schema, "item")
        );
        String expected = Strings.join(
            "GroupScan_Default(shallow hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.item -> [sch.customer, sch.order])",
            "Flatten_HKeyOrdered(sch.customer RIGHT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_OI_fromO() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_sku_date"),
                rowType(ais, schema, "order")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.order -> [sch.customer])",
            "Flatten_HKeyOrdered(sch.customer RIGHT sch.order)",
            "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_OI_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_sku_date"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
                "GroupScan_Default(deep hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
                "Flatten_HKeyOrdered(sch.customer RIGHT sch.order)",
                "Flatten_HKeyOrdered(flatten(sch.customer, sch.order) LEFT sch.item)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_AC_fromC() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_street_name"),
                rowType(ais, schema, "customer")
        );
        String expected = Strings.join(
            "GroupScan_Default(deep hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.address)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_AC_fromA() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_street_name"),
                rowType(ais, schema, "address")
        );
        String expected = Strings.join(
            "GroupScan_Default(shallow hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.address -> [sch.customer])",
            "Flatten_HKeyOrdered(sch.customer LEFT sch.address)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
    }

    @Test
    public void giUpdatePlan_A_fromA() {
        AkibanInformationSchema ais = coia();
        Schema schema = schema(ais);
        MaintenancePlan plan = MaintenancePlanCreator.createGroupIndexMaintenancePlan(
                schema,
                gi(ais, "gi_street"),
                rowType(ais, schema, "address")
        );
        String expected = Strings.join(
            "GroupScan_Default(shallow hkey-bound scan on _akiban_sch_customer NO_LIMIT)",
            "AncestorLookup_Default(sch.address -> [sch.customer])",
            "Flatten_HKeyOrdered(sch.customer RIGHT sch.address)"
        );
        assertEquals("plan description", expected, plan.plan().describePlan());
        //assertEquals("plan placeholder ancestor", "☃", plan.flattenedParentRowType().toString());
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

    private AkibanInformationSchema coia() {
        AkibanInformationSchema ais = AISBBasedBuilder.create(SCHEMA_NAME)
                .userTable("customer")
                    .colLong("cid")
                    .colString("name", 32)
                    .colLong("priority")
                    .pk("cid")
                .userTable("order")
                    .colLong("oid")
                    .colLong("c_id")
                    .colLong("date")
                    .colString("description", 128)
                    .pk("oid")
                    .joinTo("customer").on("c_id", "cid")
                .userTable("item")
                    .colLong("iid")
                    .colLong("o_id")
                    .colLong("sku")
                    .colLong("quantity")
                    .pk("iid")
                    .joinTo("order").on("o_id", "oid")
                .userTable("address")
                    .colLong("aid")
                    .colLong("c_id")
                    .colString("street", 256)
                    .colString("state", 2)
                    .pk("aid")
                    .joinTo("customer").on("c_id", "cid")
                .groupIndex("gi_name").on("customer", "name")
                .groupIndex("gi_name_sku").on("customer", "name").and("item", "sku")
                .groupIndex("gi_date_name_sku").on("customer", "name").and("item", "sku")
                .groupIndex("gi_sku_date").on("item", "sku").and("order", "date")
                .groupIndex("gi_street_name").on("address", "street").and("customer", "name")
                .groupIndex("gi_street").on("address", "street")
                .ais();
        SCHEMA_FACTORY.rowDefCache(ais); // create the RowDefCache, and attach RowDefs to the AIS's tables
        return ais;
    }

    // consts

    private static final String SCHEMA_NAME = "sch";
    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory();
}
