import csv
import database as db

PW = "mysql@1582"  # IMPORTANT! Put your MySQL Terminal password here.
ROOT = "root"  # IMPORTANT! Put your MySQL Terminal username here.
DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever you like.
LOCALHOST = "127.0.0.1"  # considering you have installed MySQL server on your computer

RELATIVE_CONFIG_PATH = 'D:/ecommerce-data-storage-main/config/'

USER = 'users'
PRODUCTS = 'products'
ORDER = 'orders'

connection = db.create_server_connection(LOCALHOST, ROOT, PW)

# creating the schema in the DB
db.create_and_switch_database(connection, DB, DB)

print(db.create_db_connection(LOCALHOST,ROOT,PW,DB))

# Create the tables through python code here
# if you have created the table in UI, then no need to define the table structure
# If you are using python to create the tables, call the relevant query to complete the creation

# Query to create users table
CREATE_USERS_TABLE = F'''
CREATE TABLE IF NOT EXISTS `{DB}`.`users` (
  `user_id` VARCHAR(15) NOT NULL,
  `user_name` VARCHAR(45) NOT NULL,
  `user_email` VARCHAR(45) NOT NULL,
  `user_password` VARCHAR(45) NOT NULL,
  `user_address` VARCHAR(45) NULL,
  `is_vendor` TINYINT(1) NULL DEFAULT 0,
  PRIMARY KEY (`user_id`));
'''

# Query to create orders table
CREATE_ORDERS_TABLE = f'''
CREATE TABLE IF NOT EXISTS `{DB}`.`orders` (
  `order_id` INT NOT NULL,
  `order_quantity` INT NOT NULL,
  `total_value` FLOAT(45) NOT NULL,
  `reward_point` INT NOT NULL,
  `customer_id` VARCHAR(15) NOT NULL,
  `vendor_id` VARCHAR(15) NOT NULL,
  PRIMARY KEY (`order_id`),
  CONSTRAINT `vendor_id`
    FOREIGN KEY (`vendor_id`)
    REFERENCES `{DB}`.`users` (`user_id`),
  CONSTRAINT `customer_id`
    FOREIGN KEY (`customer_id`)
    REFERENCES `{DB}`.`users` (`user_id`)
);
'''

# Query to create products table
CREATE_PRODUCTS_TABLE = f'''
CREATE TABLE IF NOT EXISTS `{DB}`.`products` (
  `product_id` VARCHAR(45) NOT NULL,
  `product_name` VARCHAR(45) NOT NULL,
  `product_description` VARCHAR(120) NOT NULL,
  `product_price` FLOAT(45) NOT NULL,
  `emi_available` VARCHAR(10) NOT NULL,
  `vendor_id` VARCHAR(15) NOT NULL,
  PRIMARY KEY (`product_id`),
  CONSTRAINT `fk_vendor_id`
    FOREIGN KEY (`vendor_id`)
    REFERENCES `{DB}`.`users` (`user_id`)
);
'''

# Query to create customer_leaderboard table
CREATE_CUSTOMER_LEADERBOARD_TABLE = f'''
CREATE TABLE IF NOT EXISTS `{DB}`.`customer_leaderboard` (
  `customer_id` VARCHAR(15) NOT NULL,
  `customer_name` VARCHAR(45) NOT NULL,
  `customer_email` VARCHAR(45) NOT NULL,
  `total_value` FLOAT(45) NOT NULL,
  PRIMARY KEY (`customer_id`),
  CONSTRAINT `fk_customer_id`
    FOREIGN KEY (`customer_id`)
    REFERENCES `{DB}`.`users` (`user_id`)
);
'''

# Creating tables 
print('create users table')
db.create_table(connection, CREATE_USERS_TABLE)
print('table users created successfully')

print('create products table')
db.create_table(connection, CREATE_PRODUCTS_TABLE)
print('table products created successfully')

print('create orders table')
db.create_table(connection, CREATE_ORDERS_TABLE)
print('table orders created successfully')

print('create leaderboard table')
db.create_table(connection, CREATE_CUSTOMER_LEADERBOARD_TABLE)
print('table leaderboard created successfully')

# End of creating tables

with open(RELATIVE_CONFIG_PATH + USER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)
    sql = '''
    INSERT INTO users (user_id, user_name, user_email, user_password, user_address, is_vendor)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    db.insert_many_records(connection, sql, val)

with open(RELATIVE_CONFIG_PATH + PRODUCTS + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))
    val.pop(0)
    sql = '''
    INSERT INTO products (product_id,product_name,product_price,product_description,vendor_id,emi_available)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    db.insert_many_records(connection, sql, val)

with open(RELATIVE_CONFIG_PATH + ORDER + '.csv', 'r') as f:
    val = []
    data = csv.reader(f)
    for row in data:
        val.append(tuple(row))

    val.pop(0)
    sql = '''
    INSERT INTO orders (order_id,customer_id,vendor_id,total_value,order_quantity,reward_point)
    VALUES (%s, %s, %s, %s, %s, %s);
    '''
    db.insert_many_records(connection, sql, val)

# ----------------------------2.b---------------------
query = '''
INSERT INTO orders
VALUES (101, 5, 134762, 20, 7, 3),
(102, 1, 12345, 4, 3, 7),
(103, 8, 189654, 40, 9, 1),
(104, 0, 9872, 0, 8, 4),
(105, 3, 12498, 12, 6, 2);
'''

db.create_insert_query(connection, query)

#---------------------------2.c------------------------
query = '''
SELECT * FROM orders;
'''
orders = db.select_query(connection, query)

connection.close()

