/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.ais;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Schema;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.protobuf.AISProtobuf;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;

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
            for(Table newTable : schema.getTables().values()) {
                Table oldTable = srcAIS.getTable(newTable.getName());
                if(oldTable != null) {
                    newTable.setMemoryTableFactory(oldTable.getMemoryTableFactory());
                }
            }
        }
    }
}
