
package com.akiban.sql.pg;

/** The service interface for the PostgreSQL server. */
public interface PostgresService {
    /** Get the port on which the server is listening. */
    public int getPort();

    /** Get the server itself. */
    public PostgresServer getServer();

}
