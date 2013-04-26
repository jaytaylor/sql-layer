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

package com.akiban.direct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.direct.COIDirectClasses.Iface.Customer;
import com.akiban.direct.COIDirectClasses.Iface.Order;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;
import com.akiban.sql.RegexFilenameFilter;
import com.akiban.sql.embedded.EmbeddedJDBCService;
import com.akiban.sql.embedded.EmbeddedJDBCServiceImpl;
import com.akiban.sql.embedded.JDBCConnection;

public final class DirectUpdateIT extends ITBase {

    private static final Logger LOG = LoggerFactory.getLogger(DirectUpdateIT.class.getName());

    private static final File RESOURCE_DIR = new File("src/test/resources/"
            + DirectUpdateIT.class.getPackage().getName().replace('.', '/'));

    public static final String SCHEMA_NAME = "test";

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        // JDBC service is not in test-services.
        return super.serviceBindingsProvider().bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class);
    }

    @Before
    public void setUp() throws Exception {
        startTestServices();
        loadDatabase();
        Direct.enter("test", ais());
        COIDirectClasses.registerDirect();
    }

    @After
    public void tearDown() throws Exception {
        Direct.unregisterDirectObjectClasses();
        Direct.leave();
        super.tearDownAllTables();
    }

    @Test
    public void testGetCustomerAndInterate() throws Exception {
        test(new TestExec() {
            public boolean exec() throws Exception {
                final Customer customer = new DirectIterableImpl<Customer>(
                        Customer.class, "customers", this).where("cid=1").single();
                assertEquals("Customer has cid", 1, customer.getCid());
                int orderCount = 0;
                for (Order order : customer.getOrders()) {
                    orderCount++;
                    assertEquals("Customer's Order's Customer is correct", customer, order.getCustomer());
                }
                assertEquals("Customer 1 has 2 orders", 2, orderCount);
                return true;
            }
        });
    }

    @Test
    public void insertNewOrderExpectSuccess() throws Exception {
        test(new TestExec() {
            public boolean exec() throws Exception {
                final Customer customer = new DirectIterableImpl<Customer>(
                        Customer.class, "customers", this).where("cid=1").single();
                final Order newOrder = customer.getOrders().newInstance();

                newOrder.setOid(103);
                newOrder.setOdate(java.sql.Date.valueOf("2011-03-03"));
                newOrder.save();

                int orderCount = 0;
                for (Order o : customer.getOrders()) {
                    orderCount++;
                    assertEquals("Customer's Order's Customer is correct", customer, o.getCustomer());
                }
                assertEquals("Customer 1 now has 3 orders", 3, orderCount);
                return true;
            }
        });
    }

    @Test
    public void updateOrderOnce() throws Exception {
        test(new TestExec() {
            public boolean exec() throws Exception {
                final Customer customer = new DirectIterableImpl<Customer>(
                        Customer.class, "customers", this).where("cid=1").single();
                final Order order = customer.getOrder(101);
                assertNotNull("Customer 1 has an order with oid=101", order);

                order.setOid(103);
                order.setOdate(java.sql.Date.valueOf("2011-03-03"));
                order.save();

                int orderCount = 0;
                for (Order o : customer.getOrders()) {
                    orderCount++;
                    assertEquals("Customer's Order's Customer is correct", customer, o.getCustomer());
                }
                assertEquals("Customer 1 now has 2 orders", 2, orderCount);
                return true;
            }
        });
    }

    @Test
    public void updateMultipleOrders() throws Exception {
        test(new TestExec() {
            public boolean exec() throws Exception {
                final Customer customer = new DirectIterableImpl<Customer>(
                        Customer.class, "customers", this).where("cid=1").single();
                for (final Order order : customer.getOrders()) {
                    Date newDate = Date.valueOf("2013-01-" + order.getOdate().toString().substring(8));
                    order.setOdate(newDate);
                    order.save();
                }
                int orderCount = 0;
                Timestamp after = Timestamp.valueOf("2012-12-31 23:59:59");
                for (Order o : customer.getOrders()) {
                    if (o.getOdate().after(after)) {
                        orderCount++;
                    }
                }
                assertEquals("Customer 1 now has 2 orders in 2013", 2, orderCount);
                return true;
            }
        });
    }

    @Test
    public void insertNewOrderExpectFailure() throws Exception {
        test(new TestExec() {
            public boolean exec() throws Exception {
                final Customer customer = new DirectIterableImpl<Customer>(
                        Customer.class, "customers", this).where("cid=1").single();
                final Order newOrder = customer.getOrders().newInstance();
                newOrder.setOid(101); // will collide
                newOrder.setOdate(new java.sql.Date(System.currentTimeMillis()));
                try {
                    newOrder.save();
                    fail("Should have failed");
                } catch (DirectException e) {
                    assertTrue("Should wrap a SQLException",
                            SQLException.class.isAssignableFrom(e.getCause().getClass()));
                } catch (Exception e) {
                    fail("Wrong type of exception: " + e);
                }
                return false;
            }
        });
    }

    private void test(TestExec te) throws Exception {
        JDBCConnection conn = (JDBCConnection) Direct.getContext().getConnection();
        conn.beginTransaction();
        boolean commit = false;
        try {
            commit = te.exec();
        } finally {
            if (commit) {
                conn.commitTransaction();
            }
        }
    }

    interface TestExec extends DirectObject {
        boolean exec() throws Exception;
    }

    private void loadDatabase() throws Exception {
        File schemaFile = new File(RESOURCE_DIR, "schema.ddl");
        if (schemaFile.exists()) {
            LOG.info("Loading " + schemaFile);
            loadSchemaFile(SCHEMA_NAME, schemaFile);
        }
        for (File data : RESOURCE_DIR.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            LOG.info("Loading " + data);
            loadDataFile(SCHEMA_NAME, data);
        }
    }
}
