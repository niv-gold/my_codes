def main() -> None:

    # ── CREATE ─────────────────────────────────────────────────────────────────
    employee = {
        "employee_id": 101,
        "first_name": "Niv",
        "last_name": "Goldberg",
        "department": "Data Engineering",
        "salary": 120000,
        "skills": ["Python", "PySpark", "SQL"],
        "is_active": True,
    }

    # ── READ ───────────────────────────────────────────────────────────────────
    print("── READ ──────────────────────────────────────────")
    print(employee["first_name"])                          # direct key access
    print(employee.get("manager", "No manager assigned"))  # safe get with default
    print(employee.keys())                                 # all keys
    print(employee.values())                               # all values
    print(employee.items())                                # key-value pairs

    # ── UPDATE ────────────────────────────────────────────────────────────────
    print("\n── UPDATE ────────────────────────────────────────")
    employee["salary"] = 135000                            # update existing key
    employee["city"] = "Tel Aviv"                          # add new key
    employee["skills"].append("dbt")                       # mutate nested list
    print(employee)

    # ── DELETE ────────────────────────────────────────────────────────────────
    print("\n── DELETE ────────────────────────────────────────")
    removed = employee.pop("city")                         # removes + returns value
    print(f"Removed: {removed}")
    print(employee)

    # ── LOOP ──────────────────────────────────────────────────────────────────
    print("\n── LOOP ──────────────────────────────────────────")
    for key, value in employee.items():
        print(f"  {key:15s} → {value}")

    # ── NESTED DICT ───────────────────────────────────────────────────────────
    print("\n── NESTED DICT ───────────────────────────────────")
    team = {
        "emp_101": {"name": "Niv",   "role": "Senior DE"},
        "emp_102": {"name": "Dana",  "role": "Data Analyst"},
        "emp_103": {"name": "Rotem", "role": "Analytics Engineer"},
    }
    for emp_id, details in team.items():
        print(f"  {emp_id} → {details['name']} | {details['role']}")

    # ── DICT COMPREHENSION ────────────────────────────────────────────────────
    print("\n── DICT COMPREHENSION ────────────────────────────")
    salaries = {"Niv": 135000, "Dana": 98000, "Rotem": 110000}
    after_raise = {name: round(sal * 1.10) for name, sal in salaries.items()}
    print(after_raise)

    # ── MERGE TWO DICTS ───────────────────────────────────────────────────────
    print("\n── MERGE TWO DICTS ───────────────────────────────")
    base_info   = {"employee_id": 101, "name": "Niv"}
    extra_info  = {"department": "Data Engineering", "city": "Tel Aviv"}
    merged = {**base_info, **extra_info}               # unpack operator
    print(merged)

    # ── CHECK KEY EXISTS ──────────────────────────────────────────────────────
    print("\n── CHECK KEY EXISTS ──────────────────────────────")
    print("salary" in employee)                        # True
    print("manager" in employee)                       # False

def dict_sandbox()-> None:
    employee = {
        "employee_id": 101,
        "first_name": "Niv",
        "last_name": "Goldberg",
        "department": "Data Engineering",
        "salary": 120000,
        "skills": ["Python", "PySpark", "SQL"],
        "is_active": True,
    }

    # add key:value to dict
    employee['father']='Arkadi' 
    # update key
    employee["salary"] = 555
    # add to list value an element
    employee["skills"].append('AirFlow') 
    #  print all keys
    for k,v in employee.items():
        print(f'key: {k} ; value: {v}')
    # unpacking dict
    base_info   = {"employee_code": 38, "last_name": "Goldberg"}
    unp = {**employee, **base_info}
    print(unp)


if __name__ == "__main__":
    # main()    
    dict_sandbox()