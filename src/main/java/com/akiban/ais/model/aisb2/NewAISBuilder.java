package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AkibanInformationSchema;

public interface NewAISBuilder {
    AkibanInformationSchema ais();
    NewAISBuilder defaultSchema(String schema);
    NewUserTableBuilder userTable(String table);
    NewUserTableBuilder userTable(String schema, String table);
}
