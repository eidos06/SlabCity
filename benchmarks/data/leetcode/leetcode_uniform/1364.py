import random
from database_generator.database_generator import DatabaseGenerator, generate_pair
import sys

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size =dbsize, script_file=__file__,
                                 problem_id="1364")

# setup seed
seed = 2333
random.seed(seed)

size = 1000000

if dbsize == '10M':
    size = 10000000
elif dbsize == '1M':
    size = 1000000
elif dbsize == '100K':
    size = 100000


"""
1364 schema

Table: Customers

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| customer_id   | int     |
| customer_name | varchar |
| email         | varchar |
+---------------+---------+
customer_id is the primary key for this table.
Each row of this table contains the name and the email of a customer of an online shop.

Table: Contacts

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| user_id       | id      |
| contact_name  | varchar |
| contact_email | varchar |
+---------------+---------+
(user_id, contact_email) is the primary key for this table.
Each row of this table contains the name and email of one contact of customer with user_id.
This table contains information about people each customer trust. The contact may or may not exist in the Customers table.

Table: Invoices

+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| invoice_id   | int     |
| price        | int     |
| user_id      | int     |
+--------------+---------+
invoice_id is the primary key for this table.
Each row of this table indicates that user_id has an invoice with invoice_id and a price.

"""

# setup columns
cus_row = int(size / 10)
con_row = size
inv_row = size

cus_id_pool = list(range(cus_row))
cus_name_pool = ['name_' + str(i) for i in range(cus_row)]
cus_email_pool = [str(i) + '@gg.com' for i in range(cus_row)]

contact_id_pool = list(range(2 * cus_row))

invoice_id = list(range(inv_row))
price_pool = [random.randint(10, 15) for _ in range(inv_row)]

# fill in columns

contacts = []
contacts.append(['user_id','contact_email','contact_name'])
contacts.append(['int','str','str'])

pkey = set()
for i in range(con_row):
    pair = generate_pair(cus_id_pool, contact_id_pool)
    while pair in pkey or pair[0] == pair[1]:
        pair = generate_pair(cus_id_pool, contact_id_pool)
    pkey.add(pair)
    contacts.append([pair[0], 'name_' + str(pair[1]), str(pair[1]) + '@gg.com'])

inv_user_id = [random.choice(cus_id_pool) for i in range(inv_row)]

# prepare table

customers = list(zip(cus_id_pool, cus_name_pool, cus_email_pool))
customers.reverse()
customers.append(['int','str','str'])
customers.append(['customer_id','customer_name','email'])
customers.reverse()

invoices = list(zip(invoice_id, inv_user_id, price_pool))
invoices.reverse()
invoices.append(['int','int','int'])
invoices.append(['invoice_id','user_id','price'])
invoices.reverse()

# output tables
db_generator.output("Customers", customers)
db_generator.output("Contacts", contacts)
db_generator.output("Invoices", invoices)
