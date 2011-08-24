/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.ais.io;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.metamodel.MetaModel;
import com.akiban.ais.metamodel.ModelObject;
import com.akiban.ais.model.Source;
import com.akiban.server.error.AisSQLErrorException;

public class MySQLSource extends Source
{
    // Source interface

    public void close()
    {
        try {
        connection.close();
        } catch (SQLException ex) {
            throw new AisSQLErrorException ("MySQLSource close", ex.getMessage());
        }
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
    public int readVersion ()
    {
        // there is no version number stored in the AIS model currently. 
        return MetaModel.only().getModelVersion();
    }

    @Override
    protected final void read(String typename, Receiver receiver)
    {
        ModelObject modelObject = MetaModel.only().definition(typename);
        try {
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
        } catch (SQLException ex) {
            throw new AisSQLErrorException ("MySQLSource read", ex.getMessage());
        }
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
