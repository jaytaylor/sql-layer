select description 
  from animal left join mammal on mammal.animal = animal.id
  where mammal.pregnant = 1