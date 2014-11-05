select
human0_.mammal as col_0_0_, human0_2_.description as col_1_0_
from Human human0_
inner join Mammal human0_1_ on human0_.mammal=human0_1_.animal
inner join Animal human0_2_ on human0_.mammal=human0_2_.id left outer
join Human_friends friends1_ on human0_.mammal=friends1_.human1 left outer
join Human human2_ on friends1_.human2=human2_.mammal left outer
join Mammal human2_1_ on human2_.mammal=human2_1_.animal left outer
join Animal human2_2_ on human2_.mammal=human2_2_.id
where human0_.mammal in
(
   select
   human3_.mammal
   from Human human3_
   inner join Mammal human3_1_ on human3_.mammal=human3_1_.animal
   inner join Animal human3_2_ on human3_.mammal=human3_2_.id left outer
   join Human_friends friends4_ on human3_.mammal=friends4_.human1 left outer
   join Human human5_ on friends4_.human2=human5_.mammal left outer
   join Mammal human5_1_ on human5_.mammal=human5_1_.animal left outer
   join Animal human5_2_ on human5_.mammal=human5_2_.id
   where human3_.mammal=1
)
