-- Make sure that this is ordered correctly
-- at the time of this writing it produced the correct result, but with a sort on
-- the outside. The ideal solution might involve putting the m2m index scan on the
-- outside and then the outer sort can be removed, but that optimization is for
-- another day.
SELECT b.id AS b_id, b.data AS b_data, anon_1.id AS anon_1_id, m2m_1.id AS m2m_1_id
    FROM a AS anon_1
    JOIN m2m AS m2m_1 ON anon_1.id = m2m_1.aid
    JOIN b ON b.id = m2m_1.bid
    ORDER BY anon_1.id, m2m_1.id
