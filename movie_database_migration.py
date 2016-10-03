import psycopg2
import csv

connection = psycopg2.connect("dbname=movie_database user=movie_database")
cursor = connection.cursor()

cursor.execute("DROP TABLE IF EXISTS user_data;")

create_table_command = """
CREATE TABLE user_data
(
    user_id SERIAL PRIMARY KEY NOT NULL,
    age INT,
    gender VARCHAR(1),
    occupation VARCHAR(30),
    zipcode VARCHAR(10)
);
"""

cursor.execute(create_table_command)
with open ("movie_user.csv") as open_file:
    contents = csv.reader(open_file, delimiter = "|")
    for row in contents:
        cursor.execute("INSERT INTO user_data VALUES (%s, %s, %s, %s, %s);", (row[0], row[1], row[2], row[3], row[4]))
connection.commit()

cursor.execute("DROP TABLE IF EXISTS movie_data;")
create_table_command = """
CREATE TABLE movie_data
(
    movie_id SERIAL PRIMARY KEY NOT NULL,
    movie_title VARCHAR(100),
    release_date VARCHAR(30),
    video_release_date VARCHAR(30),
    IMDb_url VARCHAR(150),
    unknown VARCHAR(50),
    action_genre INT,
    adventure INT,
    animation INT,
    children INT,
    comedy INT,
    crime INT,
    documentary INT,
    drama INT,
    fantasy INT,
    film_noir INT,
    horror INT,
    musical INT,
    mystery INT,
    romance INT,
    sci_fi INT,
    thriller INT,
    war INT,
    western INT
);
"""

cursor.execute(create_table_command)
with open ("movie_item.csv") as open_file:
    contents = csv.reader(open_file, delimiter = "|")
    for row in contents:
        cursor.execute("INSERT INTO movie_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
        (row[:]))
connection.commit()

cursor.execute("DROP TABLE IF EXISTS user_info;")
create_table_command = """
CREATE TABLE user_info(
    user_id INT REFERENCES user_data(user_id),
    item_id INT REFERENCES movie_data(movie_id),
    ratings INT,
    time_stamp INT
);
"""

cursor.execute(create_table_command)
with open("movie_data.csv") as open_file:
    contents = csv.reader(open_file, delimiter = "\t")
    for row in contents:
        cursor.execute("INSERT INTO user_info VALUES (%s, %s, %s, %s);", (row[:]))
connection.commit()

cursor.close()
connection.close()
