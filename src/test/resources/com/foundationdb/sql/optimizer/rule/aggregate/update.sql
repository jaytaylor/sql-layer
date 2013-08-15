UPDATE items SET quan = (SELECT MAX(quan) FROM items)
