select rooms.name, MAX(extract(year from CURRENT_DATE) - extract(year from students.birthday)) -MIN(extract(year from CURRENT_DATE) - extract(year from students.birthday)) as age_difference
from rooms
join students on rooms.id = students.room_id
group by rooms.id
order by age_difference desc
limit 5