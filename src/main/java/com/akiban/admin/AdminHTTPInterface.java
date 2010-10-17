package com.akiban.admin;

import org.mortbay.jetty.Server;

public class AdminHTTPInterface
{
    public static void main(String[] args) throws Exception
    {
        Server server = new Server(DEFAULT_ADMIN_HTTP_PORT);
        server.setHandler(new AdminHTTPHandler());
        server.start();
        server.join();
    }

    private static final int DEFAULT_ADMIN_HTTP_PORT = 8765;
}
