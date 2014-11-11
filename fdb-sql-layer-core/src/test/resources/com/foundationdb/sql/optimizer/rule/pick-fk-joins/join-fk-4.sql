select animal.description, mammal.name, reptile.name
from animal left join reptile on (reptile.animal = animal.id)
left join mammal on (mammal.animal = animal.id)
where animal.zoo_id=100

