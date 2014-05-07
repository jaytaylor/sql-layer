DELETE FROM x WHERE EXISTS (SELECT * FROM x ox WHERE x.v1 = ox.v1 AND x.xid <> ox.xid)
