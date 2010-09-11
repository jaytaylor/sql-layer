package com.akiban.cserver.manage;

import com.akiban.ais.model.AkibaInformationSchema;

public interface SchemaManager extends SchemaMXBean {

    /**
     * Gets a copy of the current AIS, in POJO form.
     * @return the ais
     */
    public AkibaInformationSchema getAisCopy();
}
