import random
from database_generator.database_generator import DatabaseGenerator
from faker import Faker
import datetime
import sys

def generate_pair(pool1, pool2):
    p1 = random.choice(pool1)
    p2 = random.choice(pool2)
    return (p1, p2)

dbsize = sys.argv[1]

# some setups
db_generator = DatabaseGenerator(size=dbsize, script_file=__file__,
                                 problem_id="calcite_sales")

seed = 2333
Faker.seed(seed)
fake = Faker()
random.seed(seed)

size = 1000000

if dbsize == 'calcite_4M':
    size = 4000000
elif dbsize == 'calcite_1M':
    size = 1000000
elif dbsize == 'calcite_250K':
    size = 250000


emp_num = size
dept_num = int(size / 100)
ename_num = int(emp_num / 100)
dname_num = int(dept_num / 10)
mgr_num = int(emp_num / 100)
date_num = 1000
salary_num = 1000
comm_num = 1000
null_prob = 0.1


empno = list(range(emp_num))
deptno = list(range(dept_num))

ename_pool = ['ename_' + str(i) for i in range(ename_num)]
ename_pool.append('foo')
ename_pool.append('bar')
ename_pool.append('John')
ename_pool.append('SMITH')
ename_pool.append('A')
ename_pool.append('ok')
ename_pool.append('rookie')

dname_pool = ['dname_' + str(i) for i in range(dname_num)]
dname_pool.append('CLERK')
dname_pool.append('Charlie')
dname_pool.append('Propane')



mgr_pool = random.choices(empno, k=mgr_num)

date_pool = [fake.date_between(datetime.date(2009,1,1), datetime.date(2022,11,30)) for _ in range(date_num)]
date_pool.append(datetime.date(2022,1,1))
date_pool.append(datetime.date(2020,12,11))

birth_date_pool = [fake.date_between(datetime.date(2000,1,1), datetime.date(2015,11,30)) for _ in range(date_num)]

salary_pool = random.choices(list(range(1,3000)), k=salary_num)
salary_pool.append(4)
salary_pool.append(1000)
salary_pool.append(2000)
salary_pool.append(5000)

comm_pool = random.choices(list(range(30)), k=comm_num)
comm_pool.append(10)
comm_pool.append(100)
comm_pool.append(200)
comm_pool.append(500)

slacker_pool = ['TRUE', 'FALSE']

# fill in columns

emp_table = []
emp_b_table = []
empnullables_table = []
empnullables_20_table = []

dept_table = []
bonus_table = []


emp_table.append(['EMPNO','DEPTNO','ENAME','JOB','MGR','HIREDATE','SAL','COMM','SLACKER'])
emp_table.append(['int','int','str','str','int','date','int','int','boolean'])

empnullables_table.append(['EMPNO','DEPTNO','ENAME','JOB','MGR','HIREDATE','SAL','COMM','SLACKER'])
empnullables_table.append(['int','int','str','str','int','date','int','int','boolean'])

empnullables_20_table.append(['EMPNO','DEPTNO','ENAME','JOB','MGR','HIREDATE','SAL','COMM','SLACKER'])
empnullables_20_table.append(['int','int','str','str','int','date','int','int','boolean'])

emp_b_table.append(['EMPNO','DEPTNO','ENAME','JOB','MGR','HIREDATE','SAL','COMM','SLACKER','BIRTHDATE'])
emp_b_table.append(['int','int','str','str','int','date','int','int','boolean','date'])

for i in range(emp_num):
    emp_table.append([empno[i],
                      random.choice(deptno),
                      random.choice(ename_pool),
                      random.choice(dname_pool),
                      random.choice(mgr_pool) if random.uniform(0, 1) > null_prob else '',
                      random.choice(date_pool),
                      random.choice(salary_pool),
                      random.choice(comm_pool),
                      random.choice(slacker_pool)])

    emp_b_table.append([empno[i],
                      random.choice(deptno),
                      random.choice(ename_pool),
                      random.choice(dname_pool),
                      random.choice(mgr_pool) if random.uniform(0, 1) > null_prob else '',
                      random.choice(date_pool),
                      random.choice(salary_pool),
                      random.choice(comm_pool),
                      random.choice(slacker_pool),
                      random.choice(birth_date_pool)])

    empnullables_table.append([empno[i],
                              random.choice(deptno) if random.uniform(0, 1) > null_prob else '',
                              random.choice(ename_pool) if random.uniform(0, 1) > null_prob else '',
                              random.choice(dname_pool) if random.uniform(0, 1) > null_prob else '',
                              random.choice(mgr_pool) if random.uniform(0, 1) > null_prob else '',
                              random.choice(date_pool) if random.uniform(0, 1) > null_prob else '',
                              random.choice(salary_pool) if random.uniform(0, 1) > null_prob else '',
                              random.choice(comm_pool) if random.uniform(0, 1) > null_prob else '',
                              random.choice(slacker_pool) if random.uniform(0, 1) > null_prob else ''])


for row in empnullables_table:
    if row[1] != '' and row[6] != '' and row[1] == 20 and row[6] > 1000:
        empnullables_20_table.append(row)

dept_table.append(['DEPTNO','NAME'])
dept_table.append(['int','str'])

bonus_table.append(['ENAME','JOB','SAL','COMM'])
bonus_table.append(['str','str','int','int'])

for i in range(dept_num):
    dept_table.append([deptno[i], random.choice(dname_pool)])


for i in range(size):
    bonus_table.append([random.choice(ename_pool),
                       random.choice(dname_pool),
                       random.choice(salary_pool),
                       random.choice(comm_pool)])


# output tables
db_generator.output("EMP", emp_table)
db_generator.output("EMPNULLABLES", empnullables_table)
db_generator.output("EMPNULLABLES_20", empnullables_20_table)
db_generator.output("EMP_B", emp_b_table)

db_generator.output("DEPT", dept_table)
db_generator.output("BONUS", bonus_table)


