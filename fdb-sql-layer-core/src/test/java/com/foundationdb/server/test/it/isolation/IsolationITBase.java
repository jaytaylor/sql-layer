/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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

package com.foundationdb.server.test.it.isolation;

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.sql.embedded.EmbeddedJDBCITBase;

import java.sql.Connection;
import java.sql.SQLException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public abstract class IsolationITBase extends EmbeddedJDBCITBase
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Isolation {
        public int value();
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface SQLExceptionExpected {
        public ErrorCode errorCode();
    }

    @Override
    protected boolean retryException(Throwable t) {
        // Turn off default retry in ApiTestBase.
        return false;
    }

    protected static class ExpectingSQLException extends Statement {
        private final Statement base;
        private final SQLExceptionExpected expected;
        
        public ExpectingSQLException(Statement base, SQLExceptionExpected expected) {
            this.base = base;
            this.expected = expected;
        }

        @Override
        public void evaluate() throws Throwable {
            try {
                base.evaluate();
            }
            catch (SQLException ex) {
                ErrorCode errorCode = expected.errorCode();
                if (errorCode != null) {
                    Assert.assertEquals("expected SQL state", errorCode.getFormattedValue(), ex.getSQLState());
                }
                return;
            }
            Assert.fail("Expected a SQL exception, but none was thrown");
        }
    }

    protected static class IsolationWatcher extends TestWatcher {
        private Isolation testIsolation;

        @Override
        protected void starting(Description desc) {
            testIsolation = desc.getAnnotation(Isolation.class);
        }

        @Override
        public Statement apply(Statement base, Description desc) {
            base = super.apply(base, desc);
            SQLExceptionExpected expected = desc.getAnnotation(SQLExceptionExpected.class);
            if (expected == null)
                return base;
            else
                return new ExpectingSQLException(base, expected);
        }

        public int getTestIsolationLevel() {
            if (testIsolation != null)
                return testIsolation.value();
            else
                return Connection.TRANSACTION_NONE;
        }
    }

    @Rule
    public IsolationWatcher watcher = new IsolationWatcher();

    public Connection getAutoCommitConnection() throws SQLException {
        return super.getConnection();
    }    

    @Override
    public Connection getConnection() throws SQLException {
        Connection conn = super.getConnection();
        int isolation = watcher.getTestIsolationLevel();
        if (isolation != Connection.TRANSACTION_NONE) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(isolation);
            // If the current store does not actually support the isolation
            // level needed for the test, skip it.
            Assume.assumeTrue(isolation == conn.getTransactionIsolation());
        }
        return conn;
    }

}
