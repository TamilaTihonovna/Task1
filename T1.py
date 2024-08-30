import psycopg2
import pandas as pd
import argparse
import json
import xml.etree.ElementTree as ET

def load_data(students_file, rooms_file):
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user='admin',
            password='admin',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()
        
        cur.execute("TRUNCATE TABLE rooms RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE students RESTART IDENTITY CASCADE;")
        
        students_df = pd.read_json(students_file)
        rooms_df = pd.read_json(rooms_file)
        for _, row in rooms_df.iterrows():
            cur.execute("INSERT INTO rooms (id, name) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;", (row['id'], row['name']))
        
        for _, row in students_df.iterrows():
            cur.execute("""
                INSERT INTO students (id, name, birthday, sex, room_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    birthday = EXCLUDED.birthday,
                    sex = EXCLUDED.sex,
                    room_id = EXCLUDED.room_id;
            """, (row['id'], row['name'], row['birthday'], row['sex'], row['room']))
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS name_of_room
            ON rooms (name);
        """)
        
        cur.execute("""
            CREATE INDEX IF NOT EXISTS birthday_students
            ON students (birthday);
        """)
        
        conn.commit()
        
        cur.close()
        conn.close()
        
        print("Данные успешно загружены и индексы созданы.")
        
    except Exception as e:
        print(f"Ошибка: {e}")

def generate_report(output_format):
    try:
        conn = psycopg2.connect(
            dbname='postgres',
            user='admin',
            password='admin',
            host='localhost',
            port='5432'
        )
        cur = conn.cursor()
        cur.execute("""
            select rooms.name, count(students.name) as total_students from students 
            left join rooms on rooms.id=students.room_id  
            group by rooms.id 
            order by rooms.id
        """)
        rooms_count = cur.fetchall()
        
        cur.execute("""
            select rooms.name, AVG(extract(year from CURRENT_DATE) - extract(year from students.birthday)) as average_age from students 
            join rooms on rooms.id = students.room_id 
            group by rooms.id
            order by average_age
            limit 5
        """)
        rooms_age = cur.fetchall()
        
        cur.execute("""
            select rooms.name, MAX(extract(year from CURRENT_DATE) - extract(year from students.birthday)) -MIN(extract(year from CURRENT_DATE) - extract(year from students.birthday)) as age_difference
            from rooms
            join students on rooms.id = students.room_id
            group by rooms.id
            order by age_difference desc
            limit 5
        """)
        rooms_diff = cur.fetchall()
        
        cur.execute("""
            select rooms.name from rooms 
            join students on students.room_id =rooms.id 
            group by rooms.id, rooms.name
            having count(distinct students.sex) = 2
            order by rooms.id 

        """)
        rooms_gender = cur.fetchall()

        conn.close()
        
        if output_format == 'json':
            report = {
                "rooms_count": rooms_count,
                "rooms_age": rooms_age,
                "rooms_diff": rooms_diff,
                "rooms_gender": rooms_gender
            }
            print(json.dumps(report, indent=4))
        
        elif output_format == 'xml':
            root = ET.Element("Report")
            ET.SubElement(root, "RoomsCount").text = str(rooms_count)
            ET.SubElement(root, "RoomsAge").text = str(rooms_age)
            ET.SubElement(root, "RoomsDiff").text = str(rooms_diff)
            ET.SubElement(root, "RoomsGender").text = str(rooms_gender)
            
            tree = ET.ElementTree(root)
            tree.write("report.xml")
            print("Отчет сохранен в формате XML.")

    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Загрузка данных и генерация отчетов.')
    parser.add_argument('--students', required=True, help='Путь к файлу студентов')
    parser.add_argument('--rooms', required=True, help='Путь к файлу комнат')
    parser.add_argument('--format', choices=['json', 'xml'], required=True, help='Формат вывода: json или xml')
    args = parser.parse_args()
    load_data(args.students, args.rooms)
    generate_report(args.format)
