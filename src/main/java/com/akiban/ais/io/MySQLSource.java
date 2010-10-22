package com.akiban.ais.io;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Source;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class MySQLSource extends Source
{
    // Source interface

    public void close() throws SQLException
    {
        connection.close();
    }

    // MySQLSource interface

    public  MySQLSource(final String server, final String port, final String username, final String password)
        throws Exception
    {
        Class.forName("com.mysql.jdbc.Driver");
        connection = DriverManager.getConnection("jdbc:mysql://" + server + ":" + port, username, password);
        Statement stmt = connection.createStatement();
        stmt.close();
    }

    @Override
    protected final void read(String typename, Receiver receiver) throws Exception
    {
        ModelObject modelObject = MetaModel.only().definition(typename);
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery(modelObject.readQuery());
        while (resultSet.next()) {
            Map<String, Object> map = new HashMap<String, Object>();
            int c = 0;
            for (ModelObject.Attribute attribute : modelObject.attributes()) {
                Object value = null;
                c++;
                switch (attribute.type()) {
                    case INTEGER:
                        value = integerOrNull(resultSet.getString(c));
                        break;
                    case LONG:
                        value = longOrNull(resultSet.getString(c));
                        break;
                    case BOOLEAN:
                        value = resultSet.getInt(c) != 0;
                        break;
                    case STRING:
                        value = resultSet.getString(c);
                        break;
                    default:
                        assert false;
                }
                map.put(attribute.name(), value);
            }
            receiver.receive(map);
        }
        stmt.close();
    }

    private Integer integerOrNull(String s)
    {
        return s == null ? null : Integer.parseInt(s);
    }

    private Long longOrNull(String s)
    {
        return s == null ? null : Long.parseLong(s);
    }

    // State

    private Connection connection;
}
