SELECT             id, NULL AS T3 FROM table1
  UNION ALL SELECT id, NULL AS T3 FROM table2
  UNION ALL SELECT id, T3         FROM table3