
package com.akiban.direct;

import java.sql.Connection;

public interface DirectContext {

    Connection getConnection();
    
    DirectObject getExtent();
    
}
