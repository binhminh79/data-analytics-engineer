import inspect
import os, csv
DATA_DIR = "..\data"
EMPLOYEES_CSV_PATH = os.path.abspath(f"{DATA_DIR}\employees.csv")
DEPT_EMP_CSV_PATH = os.path.abspath(f"{DATA_DIR}\dept_emp.csv")
SALARIES_CSV_PATH = os.path.abspath(f"{DATA_DIR}\salaries.csv")
DEPT_MANAGER_CSV_PATH = os.path.abspath(f"{DATA_DIR}\dept_manager.csv")

i = {'idx':0}

#util functions
import datetime

#function to calculate age
def age(birthdate_str):
    birthdate = datetime.datetime.strptime(birthdate_str, "%Y-%m-%d")
    today = datetime.date.today()
    return today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))

#function to read employees and call another function on each row
def read_file(file_path, functiontocall, i = None):
    if (functiontocall == None):
        print("Please enter exercise name")
        return
    with open(file_path, mode="r", newline="") as csvfile:
        file_reader = csv.DictReader(csvfile)
        if (i == None):
            i = {'idx':0}
        for row in file_reader:
            result, i = eval(f"{functiontocall}(row, i)")
            if not result:
                break
            i['idx'] = i['idx'] + 1
    csvfile.close()
    return not result, i


# Đọc file employees.csv và in ra màn hình tên đầy đủ của 10 nhân viên đầu tiên tính từ đầu file.

def exercise_01_helper(row,i):
    print(f"{row['first_name']} {row['last_name']}")
    if (i['idx'] >= 10):
        return False, i
    return True, i

def exercise_01():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    read_file(EMPLOYEES_CSV_PATH, "exercise_01_helper")
    return

#Đọc file employees.csv và in ra màn hình tên đầy đủ của 10 nhân viên và tuổi của nhân viên đó tình từ đầu file
def exercise_02_helper(row, i):
    print(f"{row['first_name']} {row['last_name']}, age = {age(row['birth_date'])}")
    if (i['idx'] >= 10):
        return False, i
    return True, i
def exercise_02():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    read_file(EMPLOYEES_CSV_PATH, "exercise_02_helper")
    return

#Đọc file employees.csv và in ra tên đầy đủ và tuổi của 10 nhân viên đầu tiên có tuổi >= 40 tính từ đầu file
def exercise_03_helper(row, i):
    if (age(row['birth_date']) >= 40):
        return exercise_02_helper(row, i)
    i['idx'] = i['idx'] - 1
    return True, i
def exercise_03():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    read_file(EMPLOYEES_CSV_PATH, "exercise_03_helper")
    return

#Đọc file employees.csv, in ra số lượng nhân viên nam và số lương nhân viên nữ
def exercise_04_count(row, i):
    if (row['gender'] == "F"):
        i['females'] += 1
    else:
        i['males'] += 1
    #csvfile.close()
    return True, i

def exercise_04():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    i = {'idx':0, 'females' : 0, 'males':0}
    res, i = read_file(EMPLOYEES_CSV_PATH, "exercise_04_count", i)
    print(f"males = {i['males']}, females = {i['females']}")
    return

#Đọc sử dụng 2 file employees.csv và dept_emp.csv, hãy in ra số lượng nhân viên (từ trước đến nay) của phòng ban có mã phòng ban là d001
def exercise_05_count(row, i):
    i['d001'] += 1 if row['dept_no'] == "d001" else 0
    return True, i

def exercise_05():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    i = {'idx':0, 'd001' : 0}
    res, i = read_file(DEPT_EMP_CSV_PATH, "exercise_05_count", i)
    print(f"number of d001 employees in history = {i['d001']}")
    return

#Đọc file salaries, in ra mã nhân viên có mức lương cao nhất từ trước đến nay.
def exercise_06_max(row, i):
    if int(row['salary']) > int(i['max_pay']):
        i['max_pay'] = row['salary']
        i['max_id'] = row['emp_no']
    return True, i

def exercise_06():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    i = {'idx': 0, 'max_pay': 1, 'max_id' : None}
    res, i = read_file(SALARIES_CSV_PATH, "exercise_06_max", i)
    print(f"max pay = {i['max_pay']}, from employee number {i['max_id']}")
    return

#Sử dụng file dept_manager.csv, hãy in ra mã nhân viên trưởng phòng của phòng ban d008
def exercise_07_current(row, i):
    if row['dept_no'] == i["search"] and datetime.datetime.strptime(row['to_date'], "%Y-%m-%d") >= datetime.datetime.today():
        print(f"Current manager of dept_no d008 is {row['emp_no']}")
        return False, i
    return True, i

def exercise_07(toprint = True):
    if toprint:
        print(f"----------{inspect.stack()[0][3]}-------------" )
    i = {'idx': 0, 'search':'d008'}
    res, i = read_file(DEPT_MANAGER_CSV_PATH, "exercise_07_current", i)
    if not res:
        print (f"this department {i['search']} has no current manager")
    return i

#In ra lương hiện tại của trưởng phòng của phòng ban d008
def emp_salary(row, i):
    if row['emp_no'] == i['search']:
        i['found'] = row['salary']
        return False, i
    return True, i

def exercise_08():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    found_emp = exercise_07(False)
    if (found_emp == None): return
    i = {'idx': 0, 'search':found_emp, 'found':None}
    res, i = read_file(SALARIES_CSV_PATH, "emp_salary", i)
    if res : print(f"Current manager's salary is {i['found']}")
    else : print("Current manager has no salary info")
    return

# tạo chuỗi SQL
def exercise_09():
    print(f"----------{inspect.stack()[0][3]}-------------" )
    select_columns = ["emp_no", "first_name", "last_name"]
    table_name = "sql_practice.employees"
    where_condition = {
        "gender": "F",
    }
    where_string = " and ".join(f"{key}=\"{value}\"" for key, value in where_condition.items())
    sql = f"Select {', '.join(select_columns)} from {table_name} where {where_string}"
    print (sql)
    return
#read_file(EMPLOYEES_CSV_PATH, "exercise_03")


for i in range(1, 10):
    func_name = f"exercise_0{i}()"
    eval(func_name)

