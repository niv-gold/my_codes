import pandas as pd
import fidality_data_analytic as fda

def load_excel_sheet(excel_file: str, sheet_name: str, nrows: int = None) -> pd.DataFrame:
	try:
		df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=nrows)
		print(f"Sheet '{sheet_name}' loaded successfully.")
		return df
	except FileNotFoundError:
		print(f"File not found: {excel_file}")
	except ValueError as err:
		print(f"Sheet not found or invalid Excel file: {err}")
	return pd.DataFrame()

def load_dim_employees(excel_file: str) -> pd.DataFrame:
	return load_excel_sheet(excel_file, "dim_employees")

def load_fact_sales(excel_file: str) -> pd.DataFrame:
	return load_excel_sheet(excel_file, "fact_sales")



def main() -> None: 
    try:
        excel_file = "sql_sand_box/MSSQL/data/star_schema_sales_2025.xlsx"
        df_dim_employees = load_excel_sheet(excel_file, "dim_employees")
        df_fact_sales = load_excel_sheet(excel_file, "fact_sales", nrows=2500).copy()
        print("Data loading completed successfully.")

        dim_emp_slim = df_dim_employees[["employee_id","employee_key","first_name","last_name"]]
        fact_sales_slim = df_fact_sales[["sale_id","employee_id","order_number","customer_id","gross_profit_usd"]]
        print(dim_emp_slim.head(2))
        print(fact_sales_slim.head(2))
        res_fact_sales = pd.merge(fact_sales_slim, dim_emp_slim, how="inner", on=["employee_id"], indicator=True )
        print(res_fact_sales.head(3))

    except Exception as err:
        print(f"An error occurred: {err}")
        
if __name__ == "__main__":
	main()

