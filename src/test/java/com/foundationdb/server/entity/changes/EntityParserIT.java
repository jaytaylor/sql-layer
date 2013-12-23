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
package com.foundationdb.server.entity.changes;

import static com.foundationdb.util.JsonUtils.readTree;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.test.it.ITBase;

public class EntityParserIT extends ITBase {

    private EntityParser parser;
    
    @Before
    public void createParser() {
        parser = new EntityParser();
    }
 
    
    @Test 
    public void testCustomer() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");

        String postInput = "{\"cid\": 3, \"first_name\": \"Bobby\",\"last_name\": \"Jones\"}";
        AkibanInformationSchema ais = processParse(tableName, postInput);
        
        assertEquals(3, ais.getTable(tableName).getColumns().size());
        assertEquals("cid", ais.getTable(tableName).getColumn(0).getName());
        assertEquals("BIGINT", ais.getTable(tableName).getColumn(0).tInstance().typeClass().name().unqualifiedName());
        assertEquals("first_name", ais.getTable(tableName).getColumn(1).getName());
        assertEquals("VARCHAR", ais.getTable(tableName).getColumn(1).tInstance().typeClass().name().unqualifiedName());
        assertEquals("last_name", ais.getTable(tableName).getColumn(2).getName());
        assertEquals("VARCHAR", ais.getTable(tableName).getColumn(2).tInstance().typeClass().name().unqualifiedName());
    }
    
    @Test
    public void testOrders() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "orders");
        String postInput = "{\"oid\" : 103, \"cid\" : 2, \"odate\": \"2012-12-31T12:00:00\"}";
        AkibanInformationSchema ais = processParse(tableName, postInput);

        assertEquals(3, ais.getTable(tableName).getColumns().size());
        assertEquals("BIGINT", ais.getTable(tableName).getColumn(0).tInstance().typeClass().name().unqualifiedName());
    }
    
    @Test
    public void testCA() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput ="{\"cid\": 6, \"first_name\": \"John\",\"last_name\": \"Smith\"," +
                "\"test.addresses\": {\"aid\": 104, \"cid\": 6, \"state\": \"MA\", \"city\": \"Boston\"}}";
        AkibanInformationSchema ais = processParse(tableName, postInput);
        
        Table c = ais.getTable(tableName);
        assertEquals(4, c.getColumns().size());
        assertEquals(EntityParser.PK_COL_NAME, c.getColumn(3).getName());
        assertEquals("INT", c.getColumn(3).tInstance().typeClass().name().unqualifiedName());

        tableName = new TableName ("test", "addresses");
        assertNotNull (ais.getTable(tableName));
        Table a = ais.getTable(tableName);
        assertEquals(5, a.getColumns().size());
        assertEquals("aid", a.getColumn(0).getName());
        assertEquals("cid", a.getColumn(1).getName());
        assertEquals("state", a.getColumn(2).getName());
        assertEquals("city", a.getColumn(3).getName());
        assertEquals("_customers_id", a.getColumn(4).getName());
        
        assertNotNull(a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertTrue (join.getChild() == a);
    }
    
    @Test
    public void testJsonTypes() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput ="{\"cid\": 6, \"first_name\": \"John\", \"ordered\": false, \"order_date\": null, \"tax_rate\": 0.01}";
        AkibanInformationSchema ais = processParse(tableName, postInput);

        Table c = ais.getTable(tableName);
        assertEquals(5, c.getColumns().size());
        assertEquals("cid", c.getColumn(0).getName());
        assertEquals("BIGINT", c.getColumn(0).tInstance().typeClass().name().unqualifiedName());
        assertEquals("first_name", c.getColumn(1).getName());
        assertEquals("VARCHAR", c.getColumn(1).tInstance().typeClass().name().unqualifiedName());
        assertEquals("ordered", c.getColumn(2).getName());
        assertEquals("BOOLEAN", c.getColumn(2).tInstance().typeClass().name().unqualifiedName());
        assertEquals("order_date", c.getColumn(3).getName());
        assertEquals("VARCHAR", c.getColumn(3).tInstance().typeClass().name().unqualifiedName());
        assertEquals("tax_rate", c.getColumn(4).getName());
        assertEquals("DOUBLE", c.getColumn(4).tInstance().typeClass().name().unqualifiedName());
    }
    
    @Test
    public void testCAO() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput ="{\"cid\": 6, \"first_name\": \"John\",\"last_name\": \"Smith\"," +
                "\"addresses\": {\"aid\": 104, \"cid\": 6, \"state\": \"MA\", \"city\": \"Boston\"}," +
                "\"orders\": {\"oid\" : 103, \"cid\" : 2, \"odate\": \"2012-12-31T12:00:00\"}}";
        AkibanInformationSchema ais = processParse(tableName, postInput);
        
        Table c = ais.getTable(tableName);
        
        assertNotNull (ais.getTable(new TableName("test", "addresses")));
        Table a = ais.getTable (new TableName ("test", "addresses"));
        assertNotNull (ais.getTable(new TableName("test", "orders")));
        Table o = ais.getTable(new TableName ("test", "orders"));
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertTrue (join.getChild() == a);

        join = o.getParentJoin();
        assertTrue (join.getParent() == c);
        assertTrue (join.getChild() == o);
        
    }
    
    @Test
    public void testCA_array() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput ="{\"cid\": 6, \"first_name\": \"John\",\"last_name\": \"Smith\"," +
                "\"addresses\": [{\"aid\": 104, \"cid\": 6, \"state\": \"MA\", \"city\": \"Boston\"}, " +
                "{\"aid\": 105, \"cid\": 6, \"state\": \"MA\", \"city\": \"Boston\"}]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);

        tableName = new TableName ("test", "addresses");
        assertNotNull (ais.getTable(tableName));
        Table a = ais.getTable(tableName);
        assertNotNull(a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertTrue (join.getChild() == a);
    }
    
    @Test
    public void testEmptyArray() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);
        
        Table a = ais.getTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("placeholder"));
        assertEquals("VARCHAR", a.getColumn("placeholder").tInstance().typeClass().name().unqualifiedName());
    }
    
    @Test
    public void testItemArrayInt() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[1,3,5]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);
        
        Table a = ais.getTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("value"));
        assertEquals("BIGINT", a.getColumn("value").tInstance().typeClass().name().unqualifiedName());
    }

    @Test
    public void testItemArrayString() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[\"1\",\"3\",\"5\"]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);
        
        Table a = ais.getTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("value"));
        assertEquals("VARCHAR", a.getColumn("value").tInstance().typeClass().name().unqualifiedName());
    }
    
    @Test
    public void testEmptyObject() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":{}}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);
        
        Table a = ais.getTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("placeholder"));
        assertEquals("VARCHAR", a.getColumn("placeholder").tInstance().typeClass().name().unqualifiedName());
        
    }

    @Test
    public void testEmptyArrayObject() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[{}]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);
        
        Table a = ais.getTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("placeholder"));
        assertEquals("VARCHAR", a.getColumn("placeholder").tInstance().typeClass().name().unqualifiedName());
    }

    @Test
    public void testTwoNestedJoins() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20,\"records\":[" +
                "{\"id\":\"14092603-6cf8-d791-2b72-5138fb3819d7\",\"name\":\"T-Squared Techs\","+
                "\"_acl\":{\"fields\":{}}}]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        Table c = ais.getTable(tableName);
        Table r = ais.getTable("test", "records");
        assertNotNull (r.getParentJoin());
        Table a = ais.getTable("test", "_acl");
        Table f = ais.getTable("test", "fields");
        
        assertTrue (f.getParentJoin().getParent() == a);
        assertTrue (a.getParentJoin().getParent() == r);
        assertTrue (r.getParentJoin().getParent() == c);
        assertNull (c.getParentJoin());
    }

    @Test
    public void testBoolean() throws IOException {
        TableName tableName = new TableName ("test", "customers");

        String postInput = "{\"b1\": false, \"b2\": true}";
        AkibanInformationSchema ais = processParse(tableName, postInput);
        assertEquals(2, ais.getTable(tableName).getColumns().size());
        assertEquals("b1", ais.getTable(tableName).getColumn(0).getName());
        assertEquals("BOOLEAN", ais.getTable(tableName).getColumn(0).tInstance().typeClass().name().unqualifiedName());
        assertEquals("b2", ais.getTable(tableName).getColumn(1).getName());
        assertEquals("BOOLEAN", ais.getTable(tableName).getColumn(1).tInstance().typeClass().name().unqualifiedName());
    }

    @Test
    public void testDateGood() throws IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput = "{\"cid\":6, \"order_date\":\"1970-01-01T00:00:01Z\"}";
        
        AkibanInformationSchema ais = processParse (tableName, postInput);
        assertEquals(2, ais.getTable(tableName).getColumns().size());
        assertEquals("BIGINT", ais.getTable(tableName).getColumn(0).tInstance().typeClass().name().unqualifiedName());
        assertEquals("DATETIME", ais.getTable(tableName).getColumn(1).tInstance().typeClass().name().unqualifiedName());
    }
    
    @Test
    public void testDateBad() throws IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput = "{\"cid\":6, \"phone-number\":\"802-555-1212\", \"ssn\": \"000-41-9999\"}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        assertEquals(3, ais.getTable(tableName).getColumns().size());
        assertEquals("BIGINT", ais.getTable(tableName).getColumn(0).tInstance().typeClass().name().unqualifiedName());
        assertEquals("VARCHAR", ais.getTable(tableName).getColumn(1).tInstance().typeClass().name().unqualifiedName());
        assertEquals("VARCHAR", ais.getTable(tableName).getColumn(2).tInstance().typeClass().name().unqualifiedName());
    }
    
    private AkibanInformationSchema processParse (TableName tableName, String postInput) throws JsonProcessingException, IOException{
        JsonNode node = readTree(postInput);
        parser.parseAndCreate(ddl(), session(), tableName, node);
        AkibanInformationSchema ais = dxl().ddlFunctions().getAIS(session());
        assertNotNull (ais.getTable(tableName));
        
        return ais;
    }
}
