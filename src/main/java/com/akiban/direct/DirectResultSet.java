
package com.akiban.direct;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface DirectResultSet extends ResultSet {

    DirectObject getEntity(Class<?> c) throws SQLException;
    boolean hasRow();
}
