select zoo.name, mother.name, animal.serial_number
from zoo join animal on (animal.zoo_id = zoo.id)
JOIN parent AS MOTHER on (mother.id = animal.mother_id)
where animal.description = 'Lizard'
