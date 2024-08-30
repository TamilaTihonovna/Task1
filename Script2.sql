select rooms.name, AVG(extract(year from CURRENT_DATE) - extract(year from students.birthday)) as average_age from students 
join rooms on rooms.id = students.room_id 
group by rooms.id
order by average_age
limit 5