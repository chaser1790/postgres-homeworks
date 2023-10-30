import psycopg2
import csv
import os

data_path = "D:/postgres-homeworks/homework-1/north_data/"  # Абсолютный путь к папке с данными

# Подключение к базе данных
conn = psycopg2.connect(database="north", user="postgres", password="a265845a", host="localhost")

try:
    with conn:
        with conn.cursor() as cur:

            # Загрузка данных из customers_data.csv
            customers_file_path = os.path.join(data_path, 'customers_data.csv')
            with open(customers_file_path, 'r') as customer_file:
                reader = csv.reader(customer_file)
                next(reader)
                for row in reader:
                    customer_id, company_name, contact_name = row
                    # Использую ON CONFLICT для вставки или обновления данных
                    cur.execute(
                        "INSERT INTO customers (customer_id, company_name, contact_name) VALUES (%s, %s, %s) "
                        "ON CONFLICT (customer_id) DO UPDATE SET (company_name, contact_name) = (EXCLUDED.company_name, EXCLUDED.contact_name)",
                        (customer_id, company_name, contact_name)
                    )

            # Загрузка данных из employees_data.csv
            employees_file_path = os.path.join(data_path, 'employees_data.csv')
            with open(employees_file_path, 'r') as employee_file:
                reader = csv.reader(employee_file)
                next(reader)
                for row in reader:
                    employee_id, first_name, last_name, title, birth_date, notes = row
                    # Обрезать текст в столбце "notes" до 100 символов
                    notes = notes[:100]
                    # Использую ON CONFLICT для вставки или обновления данных
                    cur.execute(
                        "INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, notes) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (employee_id) DO UPDATE SET (first_name, last_name, title, birth_date, notes) = (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.title, EXCLUDED.birth_date, EXCLUDED.notes)",
                        (employee_id, first_name, last_name, title, birth_date, notes)
                    )

            # Загрузка данных из orders_data.csv
            orders_file_path = os.path.join(data_path, 'orders_data.csv')
            with open(orders_file_path, 'r') as order_file:
                reader = csv.reader(order_file)
                next(reader)
                for row in reader:
                    order_id, customer_id, employee_id, order_date, ship_city = row
                    # Использую ON CONFLICT для вставки или обновления данных
                    cur.execute(
                        "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (order_id) DO UPDATE SET (customer_id, employee_id, order_date, ship_city) = (EXCLUDED.customer_id, EXCLUDED.employee_id, EXCLUDED.order_date, EXCLUDED.ship_city)",
                        (order_id, customer_id, employee_id, order_date, ship_city)
                    )

            print("Данные успешно загружены в таблицы.")

finally:
    conn.close()
