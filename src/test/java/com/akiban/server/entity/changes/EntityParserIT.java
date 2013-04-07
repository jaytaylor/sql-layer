/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.server.entity.changes;

import static com.akiban.util.JsonUtils.readTree;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Types;
import com.akiban.ais.model.UserTable;
import com.akiban.server.test.it.ITBase;

public class EntityParserIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(EntityParserIT.class);

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
        
        assertTrue(ais.getTable(tableName).getColumns().size() == 3);
        assertTrue(ais.getTable(tableName).getColumn(0).getName().equals("cid"));
        assertTrue(ais.getTable(tableName).getColumn(0).getType().equals(Types.BIGINT));
        assertTrue(ais.getTable(tableName).getColumn(1).getName().equals("first_name"));
        assertTrue(ais.getTable(tableName).getColumn(1).getType().equals(Types.VARCHAR));
        assertTrue(ais.getTable(tableName).getColumn(2).getName().equals("last_name"));
        assertTrue(ais.getTable(tableName).getColumn(2).getType().equals(Types.VARCHAR));
    }
    
    @Test
    public void testOrders() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "orders");
        String postInput = "{\"oid\" : 103, \"cid\" : 2, \"odate\": \"2012-12-31 12:00:00\"}";
        AkibanInformationSchema ais = processParse(tableName, postInput);

        assertTrue(ais.getTable(tableName).getColumns().size() == 3);
        assertTrue(ais.getTable(tableName).getColumn(0).getType().equals(Types.BIGINT));
    }
    
    @Test
    public void testCA() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput ="{\"cid\": 6, \"first_name\": \"John\",\"last_name\": \"Smith\"," +
                "\"test.addresses\": {\"aid\": 104, \"cid\": 6, \"state\": \"MA\", \"city\": \"Boston\"}}";
        AkibanInformationSchema ais = processParse(tableName, postInput);
        
        UserTable c = ais.getUserTable(tableName);
        assertTrue(c.getColumns().size() == 4);
        assertTrue(c.getColumn(3).getName().equals("_customers_id"));
        assertTrue(c.getColumn(3).getType().equals(Types.INT));

        tableName = new TableName ("test", "addresses");
        assertNotNull (ais.getTable(tableName));
        UserTable a = ais.getUserTable(tableName);
        assertTrue(a.getColumns().size() == 5);
        assertTrue(a.getColumn(0).getName().equals("aid"));
        assertTrue(a.getColumn(1).getName().equals("cid"));
        assertTrue(a.getColumn(2).getName().equals("state"));
        assertTrue(a.getColumn(3).getName().equals("city"));
        assertTrue(a.getColumn(4).getName().equals("_customers_id"));
        
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

        UserTable c = ais.getUserTable(tableName);
        assertTrue(c.getColumns().size() == 5);
        assertTrue(c.getColumn(0).getName().equals("cid"));
        assertTrue(c.getColumn(0).getType().equals(Types.BIGINT));
        assertTrue(c.getColumn(1).getName().equals("first_name"));
        assertTrue(c.getColumn(1).getType().equals(Types.VARCHAR));
        assertTrue(c.getColumn(2).getName().equals("ordered"));
        assertTrue(c.getColumn(2).getType().equals(Types.BOOLEAN));
        assertTrue(c.getColumn(3).getName().equals("order_date"));
        assertTrue(c.getColumn(3).getType().equals(Types.VARCHAR));
        assertTrue(c.getColumn(4).getName().equals("tax_rate"));
        assertTrue(c.getColumn(4).getType().equals(Types.DOUBLE));
    }
    
    @Test
    public void testCAO() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "customers");
        String postInput ="{\"cid\": 6, \"first_name\": \"John\",\"last_name\": \"Smith\"," +
                "\"addresses\": {\"aid\": 104, \"cid\": 6, \"state\": \"MA\", \"city\": \"Boston\"}," +
                "\"orders\": {\"oid\" : 103, \"cid\" : 2, \"odate\": \"2012-12-31 12:00:00\"}}";
        AkibanInformationSchema ais = processParse(tableName, postInput);
        
        UserTable c = ais.getUserTable(tableName);
        
        assertNotNull (ais.getTable(new TableName("test", "addresses")));
        UserTable a = ais.getUserTable (new TableName ("test", "addresses"));
        assertNotNull (ais.getTable(new TableName("test", "orders")));
        UserTable o = ais.getUserTable(new TableName ("test", "orders"));
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
        UserTable c = ais.getUserTable(tableName);

        tableName = new TableName ("test", "addresses");
        assertNotNull (ais.getTable(tableName));
        UserTable a = ais.getUserTable(tableName);
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
        UserTable c = ais.getUserTable(tableName);
        
        UserTable a = ais.getUserTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("placeholder"));
        assertTrue(a.getColumn("placeholder").getType().equals(Types.VARCHAR));
    }
    
    @Test
    public void testItemArrayInt() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[1,3,5]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        UserTable c = ais.getUserTable(tableName);
        
        UserTable a = ais.getUserTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("value"));
        assertTrue(a.getColumn("value").getType().equals(Types.BIGINT));
    }

    @Test
    public void testItemArrayString() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[\"1\",\"3\",\"5\"]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        UserTable c = ais.getUserTable(tableName);
        
        UserTable a = ais.getUserTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("value"));
        assertTrue(a.getColumn("value").getType().equals(Types.VARCHAR));
    }
    
    @Test
    public void testEmptyObject() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":{}}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        UserTable c = ais.getUserTable(tableName);
        
        UserTable a = ais.getUserTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("placeholder"));
        assertTrue(a.getColumn("placeholder").getType().equals(Types.VARCHAR));
        
    }

    @Test
    public void testEmptyArrayObject() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20, \"_acl\":[{}]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        UserTable c = ais.getUserTable(tableName);
        
        UserTable a = ais.getUserTable("test", "_acl");
        assertNotNull (a);
        assertNotNull (a.getParentJoin());
        Join join = a.getParentJoin();
        assertTrue (join.getParent() == c);
        assertEquals(2, a.getColumns().size());
        assertNotNull (a.getColumn("_campaign_id"));
        assertNotNull (a.getColumn("placeholder"));
        assertTrue(a.getColumn("placeholder").getType().equals(Types.VARCHAR));
    }

    @Test
    public void testTwoNestedJoins() throws JsonProcessingException, IOException {
        TableName tableName = new TableName ("test", "campaign");
        String postInput = "{\"next_offset\":20,\"records\":[" +
                "{\"id\":\"14092603-6cf8-d791-2b72-5138fb3819d7\",\"name\":\"T-Squared Techs\","+
                "\"_acl\":{\"fields\":{}}}]}";
        AkibanInformationSchema ais = processParse (tableName, postInput);
        UserTable c = ais.getUserTable(tableName);
        UserTable r = ais.getUserTable("test", "records");
        assertNotNull (r.getParentJoin());
        UserTable a = ais.getUserTable("test", "_acl");
        UserTable f = ais.getUserTable("test", "fields");
        
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
        assertTrue(ais.getTable(tableName).getColumns().size() == 2);
        assertTrue(ais.getTable(tableName).getColumn(0).getName().equals("b1"));
        assertTrue(ais.getTable(tableName).getColumn(0).getType().equals(Types.BOOLEAN));
        assertTrue(ais.getTable(tableName).getColumn(1).getName().equals("b2"));
        assertTrue(ais.getTable(tableName).getColumn(1).getType().equals(Types.BOOLEAN));
    }

    private AkibanInformationSchema processParse (TableName tableName, String postInput) throws JsonProcessingException, IOException{
        JsonNode node = readTree(postInput);
        parser.parseAndCreate(ddl(), session(), tableName, node);
        AkibanInformationSchema ais = dxl().ddlFunctions().getAIS(session());
        assertNotNull (ais.getTable(tableName));
        
        return ais;
    }
}
