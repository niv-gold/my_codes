import pandas as pd

# Sample data
data = {
    'Date': pd.date_range(start='2021-01-01', periods=90, freq='D'),
    'Value': range(1, 91)
}
df = pd.DataFrame(data)

# Ensure the 'Date' column is of datetime type
df['Date'] = pd.to_datetime(df['Date'])

# Create year-month identifier
df['YearMonth'] = df['Date'].dt.to_period('M')

# Rank rows within each month based on 'Value' in descending order
df['MonthlyRank'] = df.groupby('YearMonth')['Value'].rank(method='first', ascending=False)

print(df.head(10))
