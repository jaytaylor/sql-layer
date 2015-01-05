UPDATE x SET v1=(SELECT SUM(v2) FROM y WHERE x.xid=y.xid)
