select rooms.name, count(students.name) as total_students from students 
left join rooms on rooms.id=students.room_id  
group by rooms.id 
order by rooms.id