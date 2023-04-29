import database as db

# Driver code
if __name__ == "__main__":
    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = "mysql@1582"  # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "ecommerce_record"  # This is the name of the database we will create in the next step - call it whatever
    # you like.
    LOCALHOST = "127.0.0.1"
    connection = db.create_server_connection(LOCALHOST, ROOT, PW)

    # creating the schema in the DB 
    db.create_and_switch_database(connection, DB, DB)

    # Start implementing your task as mentioned in the problem statement 
    # Implement all the test cases and test them by running this file

    #----------------------------------3.a--------------------------
    MAX_ORDER_VALUE_QUERY = '''
    SELECT * FROM orders
    WHERE total_value = (SELECT MAX(total_value) FROM orders);
    '''
    print('\n\nMAXIMUM ORDER VALUE DETAILS: ', end='')
    MAX_ORDER_VALUE = db.select_query(connection, MAX_ORDER_VALUE_QUERY)

    MIN_ORDER_VALUE_QUERY = '''
    SELECT * FROM orders
    WHERE total_value = (SELECT MIN(total_value) FROM orders);
    '''
    print('\n\nMINIMUM ORDER VALUE DETAILS: ', end='')
    MIN_ORDER_VALUE = db.select_query(connection, MIN_ORDER_VALUE_QUERY)

    #-----------------------------------3.b----------------------------
    GRATER_AVERAGE_TOTAL_VALUES_QUERY = '''
    SELECT * FROM orders
    WHERE total_value > (SELECT AVG(total_value) FROM orders);
    '''

    print('\n\nThe orders with value (total_value) greater than the average value (total_value) of all the orders:')

    GRATER_AVERAGE_TOTAL_VALUES = db.select_query(connection, GRATER_AVERAGE_TOTAL_VALUES_QUERY)

    #------------------------------------3.c----------------------------
    HIGHEST_PURCHASE_VALUE_PER_CUSTOMER_QUERY = '''
    SELECT o.customer_id, MAX(o.total_value), u.user_name, u.user_email
    FROM orders o
    LEFT JOIN users u
    ON o.customer_id = u.user_id
    GROUP BY o.customer_id;
    '''

    print('\n\nlist of customers, highest purchase value of each customer: ')
    HIGHEST_PURCHASE_VALUE_PER_CUSTOMER = db.select_query(connection, HIGHEST_PURCHASE_VALUE_PER_CUSTOMER_QUERY)

    sql = '''
    INSERT INTO customer_leaderboard (customer_id, total_value, customer_name, customer_email)
    VALUES (%s, %s, %s, %s)
    '''

    db.insert_many_records(connection, sql, HIGHEST_PURCHASE_VALUE_PER_CUSTOMER)

    