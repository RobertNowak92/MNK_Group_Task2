import ftplib
import patoolib
import psycopg2

FTP_HOST = "***"
FTP_USER = "***"
FTP_PASS = "***"

#connect to ftp server
ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
ftp.encoding = "utf-8"

#download required file
filename = "task.rar"
with open(filename, "wb") as file:
    ftp.retrbinary(f"RETR {filename}", file.write)

#unpack rar file
patoolib.extract_archive("task.rar")

#connect to database, create tables, load data, create necessary constraints
#create and save query as csv
with psycopg2.connect(database="***", user="***", password="***") as conn:
    with conn.cursor() as cur:
        conn.autocommit = True      
        
        cur.execute("""
                    CREATE TABLE data (
                    part_number VARCHAR(10),
                    manufacturer VARCHAR(30),
                    main_part_number VARCHAR(50),
                    category VARCHAR(50),
                    origin VARCHAR(2)
                    );
                    
                    COPY data
                    FROM 'C:/Users/robno/Documents/Github_proj/MNK_Group_Task2/task/data.csv'
                    DELIMITER ';'
                    CSV HEADER;
                    
                    DELETE FROM data a
                    USING data b
                    WHERE a.ctid < b.ctid
                        AND a.part_number = b.part_number
                        AND a.main_part_number = b.main_part_number
                        AND a.category = b.category
                        AND a.origin = b.origin;
                    
                    ALTER TABLE data
                    ADD PRIMARY KEY (part_number);
                    
                    CREATE TABLE deposit (
                    part_number VARCHAR(10) PRIMARY KEY,
                    deposit VARCHAR(10)
                    );
                    
                    COPY deposit
                    FROM 'C:/Users/robno/Documents/Github_proj/MNK_Group_Task2/task/deposit.csv'
                    DELIMITER ';'
                    CSV HEADER;
                    
                    UPDATE deposit SET
                    deposit = replace(deposit, ',', '.');
                    
                    ALTER TABLE deposit
                    ALTER COLUMN deposit TYPE numeric USING deposit::double precision;
                    
                    DELETE FROM deposit de
                    WHERE  NOT EXISTS (
                       SELECT FROM data da
                       WHERE  de.part_number = da.part_number
                       );
                    
                    ALTER TABLE deposit
                    ADD CONSTRAINT fk_part_number
                          FOREIGN KEY(part_number) 
                    	  REFERENCES data(part_number);
                    
                    CREATE TABLE price (
                    part_number VARCHAR(10) PRIMARY KEY,
                    price VARCHAR(10)
                    );
                    
                    COPY price
                    FROM 'C:/Users/robno/Documents/Github_proj/MNK_Group_Task2/task/price.csv'
                    DELIMITER ';'
                    CSV HEADER;
                    
                    UPDATE price SET
                    price = replace(price, ',', '.');
                    
                    ALTER TABLE price
                    ALTER COLUMN price TYPE numeric USING price::double precision;
                    
                    DELETE FROM price p
                    WHERE  NOT EXISTS (
                       SELECT FROM data d
                       WHERE  p.part_number = d.part_number
                       );
                    
                    ALTER TABLE price 
                    ADD CONSTRAINT fkp_part_number
                          FOREIGN KEY(part_number) 
                    	  REFERENCES data(part_number);
                    
                    CREATE TABLE quantity (
                    part_number VARCHAR(10),
                    quantity VARCHAR(10),
                    warehouse VARCHAR(10)
                    );
                    
                    COPY quantity
                    FROM 'C:/Users/robno/Documents/Github_proj/MNK_Group_Task2/task/quantity.csv'
                    DELIMITER ';'
                    CSV HEADER;
                    
                    UPDATE quantity SET 
                    part_number = replace(part_number, '"', ''),
                    quantity = replace(quantity, '"', ''),
                    warehouse = replace(warehouse, '"', '');
                    
                    DELETE FROM quantity q
                    WHERE  NOT EXISTS (
                       SELECT FROM data d
                       WHERE  q.part_number = d.part_number
                       );
                    
                    ALTER TABLE quantity
                    ADD CONSTRAINT fkq_part_number
                          FOREIGN KEY(part_number) 
                    	  REFERENCES data(part_number);
                    
                    CREATE TABLE weight (
                    part_number VARCHAR(10) PRIMARY KEY,
                    weight_unpacked FLOAT,
                    weight_packed FLOAT
                    );
                    
                    COPY weight
                    FROM 'C:/Users/robno/Documents/Github_proj/MNK_Group_Task2/task/weight.txt'
                    DELIMITER E'\t'
                    CSV HEADER;
                    
                    DELETE FROM weight w
                    WHERE  NOT EXISTS (
                       SELECT FROM data d
                       WHERE  w.part_number = d.part_number
                       );
                    
                    ALTER TABLE weight
                    ADD CONSTRAINT fkw_part_number
                          FOREIGN KEY(part_number) 
                    	  REFERENCES data(part_number);
                    
                    COPY (
                    SELECT d.main_part_number, d.manufacturer, d.category, d.origin, p.price, 
                    coalesce(de.deposit, 0) AS deposit, 
                    p.price + coalesce(de.deposit, 0) AS final_price,
                    replace(q.quantity, '>', '')::INT AS quantity FROM data d
                    INNER JOIN price p ON p.part_number = d.part_number
                    INNER JOIN quantity q ON q.part_number = d.part_number
                    LEFT JOIN deposit de ON de.part_number = d.part_number
                    WHERE 
                    (q.warehouse LIKE 'A' 
                    OR q.warehouse LIKE 'H'
                    OR q.warehouse LIKE 'J'
                    OR q.warehouse LIKE '3'
                    OR q.warehouse LIKE '9')
                    AND replace(q.quantity, '>', '')::INT != 0
                    AND p.price + coalesce(de.deposit, 0) > 2.00
                        )
                    TO 'C:/Users/robno/Documents/Github_proj/MNK_Group_Task2/Robert_Nowak_task2.csv'
                    DELIMITER ';'
                    CSV HEADER;
                    
                    DROP TABLE deposit;
                    DROP TABLE price;
                    DROP TABLE quantity;
                    DROP TABLE weight;
                    DROP TABLE data
                    
                    """)

#change directory
ftp.cwd('/complete/Robert_Nowak')

#upload file
filename = "Robert_Nowak_task2.csv"
with open(filename, "rb") as file:
    ftp.storbinary(f"STOR {filename}", file)
    
ftp.quit() 
