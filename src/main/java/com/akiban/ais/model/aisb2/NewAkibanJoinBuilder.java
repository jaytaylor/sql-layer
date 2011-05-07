package com.akiban.ais.model.aisb2;

public interface NewAkibanJoinBuilder {
    /**
     * Adds a child -&gt; parent column pair to this join.
     *
     * <p>{@linkplain #and(String, String)} is a synonym of this method.</p>
     * @param childColumn the name of the column on the child table
     * @param parentColumn  the name of the column on the parent table
     * @return this
     */
    NewAkibanJoinBuilder on(String childColumn, String parentColumn);

    /**
     * Synonym for {@link #on(String, String)}. This method is just here to make the code more "English-sounding."
     * Example: {@code build.joinTo("parent").on("child_col_1", "parent_col_1").and("child_col_2", "parent_col_2").}
     * @param childColumn the name of the column on the child table
     * @param parentColumn  the name of the column on the parent table
     * @return this
     */
    NewAkibanJoinBuilder and(String childColumn, String parentColumn);
}
