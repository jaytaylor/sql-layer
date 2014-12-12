-- Make sure that this is ordered correctly
-- The limit 1000000 is to make sure that subquery stays a subquery even if we improve the flattener
-- I tried removing the subquery and it caused this to work correctly, see m2m-order-by
SELECT b.id AS b_id, b.data AS b_data, anon_1.a_id AS anon_1_a_id, m2m_1.id AS m2m_1_id
    FROM (SELECT a.id AS a_id FROM a LIMIT 10000000) AS anon_1
    JOIN m2m AS m2m_1 ON anon_1.a_id = m2m_1.aid
    JOIN b ON b.id = m2m_1.bid
    ORDER BY anon_1.a_id, m2m_1.id
