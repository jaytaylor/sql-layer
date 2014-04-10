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

package com.foundationdb.ais;

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;

public class CAOIBuilderFiller {
    public final static String CUSTOMER_TABLE = "customer";
    public final static String ADDRESS_TABLE = "address";
    public final static String ORDER_TABLE = "order";
    public final static String ITEM_TABLE = "item";
    public final static String COMPONENT_TABLE = "component";

    public static NewAISBuilder createAndFillBuilder(String schema) {
        TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
        NewAISBuilder builder = AISBBasedBuilder.create(schema, typesTranslator);

        builder.table(CUSTOMER_TABLE).
                colBigInt("customer_id", false).
                colString("customer_name", 100, false).
                pk("customer_id");

        builder.table(ADDRESS_TABLE).
                colBigInt("customer_id", false).
                colInt("instance_id", false).
                colString("address_line1", 60, false).
                colString("address_line2", 60, false).
                colString("address_line3", 60, false).
                pk("customer_id", "instance_id").
                joinTo("customer").on("customer_id", "customer_id");

        builder.table(ORDER_TABLE).
                colBigInt("order_id", false).
                colBigInt("customer_id", false).
                colInt("order_date", false).
                pk("order_id").
                joinTo("customer").on("customer_id", "customer_id");

        builder.table(ITEM_TABLE).
                colBigInt("order_id", false).
                colBigInt("part_id", false).
                colInt("quantity", false).
                colInt("unit_price", false).
                pk("part_id").
                joinTo("order").on("order_id", "order_id");

        builder.table(COMPONENT_TABLE).
                colBigInt("part_id", false).
                colBigInt("component_id", false).
                colInt("supplier_id", false).
                colInt("unique_id", false).
                colString("description", 50, true).
                pk("component_id").
                uniqueKey("uk", "unique_id").
                key("xk", "supplier_id").
                joinTo("item").on("part_id", "part_id");

        return builder;
    }
}
