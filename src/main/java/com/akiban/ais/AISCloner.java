
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
