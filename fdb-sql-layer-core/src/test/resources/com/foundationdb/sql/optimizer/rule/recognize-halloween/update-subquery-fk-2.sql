UPDATE x SET xid=xid+1, v1=(SELECT SUM(v2) FROM y WHERE x.xid=y.xid)
