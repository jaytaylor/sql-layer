
package com.akiban.ais.model.aisb2;

import java.util.Properties;

public interface NewViewBuilder extends NewUserTableBuilder {
    NewViewBuilder definition(String definition);

    NewViewBuilder definition(String definition, Properties properties);

    NewViewBuilder references(String table);

    NewViewBuilder references(String schema, String table, String... columns);
}
