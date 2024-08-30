select rooms.name from rooms 
join students on students.room_id =rooms.id 
group by rooms.id, rooms.name
having count(distinct students.sex) = 2
order by rooms.id 
