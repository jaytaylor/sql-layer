SELECT name,
  UPPER(name), LOWER(name), UCASE(name), LCASE(name),
  TRIM(name), LTRIM(name), RTRIM(name), SUBSTR(name,1,2),
  LENGTH(name), LOCATE(name, 'bar')
FROM parent
  