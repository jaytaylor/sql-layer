select zoo.name, mother.name, animal.serial_number
from parent AS MOTHER join animal on (mother.id = animal.mother_id)
JOIN zoo ON (animal.zoo_id = zoo.id)
where animal.description = 'Lizard'
