select description 
  from animal join mammal on mammal.animal = animal.id
  where mammal.pregnant = 1