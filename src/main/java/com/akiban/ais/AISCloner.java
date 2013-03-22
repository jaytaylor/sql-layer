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

package com.akiban.ais;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Schema;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.protobuf.AISProtobuf;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.ais.protobuf.ProtobufWriter;

public class AISCloner {
    public static AkibanInformationSchema clone(AkibanInformationSchema ais) {
        return clone(ais, ProtobufWriter.ALL_SELECTOR);
    }

    public static AkibanInformationSchema clone(AkibanInformationSchema ais, ProtobufWriter.WriteSelector selector) {
        AkibanInformationSchema newAIS = new AkibanInformationSchema();
        clone(newAIS, ais, selector);
        return newAIS;
    }

    public static void clone(AkibanInformationSchema destAIS, AkibanInformationSchema srcAIS, ProtobufWriter.WriteSelector selector) {
        ProtobufWriter writer = new ProtobufWriter(selector);
        AISProtobuf.AkibanInformationSchema pbAIS = writer.save(srcAIS);
        ProtobufReader reader = new ProtobufReader(destAIS, pbAIS.toBuilder());
        reader.loadAIS();
        // Preserve the memory table factories for any I_S tables
        Schema schema = destAIS.getSchema(TableName.INFORMATION_SCHEMA);
        if(schema != null) {
            for(UserTable newTable : schema.getUserTables().values()) {
                UserTable oldTable = srcAIS.getUserTable(newTable.getName());
                if(oldTable != null) {
                    newTable.setMemoryTableFactory(oldTable.getMemoryTableFactory());
                }
            }
        }
    }
}
