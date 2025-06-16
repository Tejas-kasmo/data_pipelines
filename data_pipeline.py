import pandas as pd
import pyodbc
import numpy as np

# 1. Load the dataset
df = pd.read_csv(r"C:\Users\mysur\.cache\kagglehub\datasets\marshalpatel3558\diabetes-prediction-dataset\versions\1\diabetes_dataset.csv")

# 2. Sex column: Female = 0.1, Male = 0.0
df['Sex'] = df['Sex'].map({'Female': 0.1, 'Male': 0.0})

# 3. Ethnicity column: map to normalized values between 0 and 1
ethnicity_unique = df['Ethnicity'].dropna().unique()
ethnicity_mapping = {eth: round(i / (len(ethnicity_unique) - 1), 2) for i, eth in enumerate(sorted(ethnicity_unique))}
df['Ethnicity'] = df['Ethnicity'].map(ethnicity_mapping)

# 4. Physical_Activity_Level column
df['Physical_Activity_Level'] = df['Physical_Activity_Level'].map({
    'Low': 0.0,
    'Moderate': 0.1,
    'High': 0.2
})

# 5. Alcohol_Consumption column
df['Alcohol_Consumption'] = df['Alcohol_Consumption'].map({
    'None': 0.0,
    'Moderate': 0.1,
    'Heavy': 0.2
})

# 6. Smoking_Status column
df['Smoking_Status'] = df['Smoking_Status'].map({
    'Never': 0.0,
    'Former': 0.1,
    'Current': 0.2
})

# 7. Fill missing values
for col in df.columns:
    if df[col].isnull().sum() > 0:
        if df[col].dtype in ['float64', 'int64']:
            df[col].fillna(df[col].mean(), inplace=True)
        else:
            df[col].fillna(df[col].mode()[0], inplace=True)

# 8. Normalize all numeric columns using min-max scaling
numeric_cols = df.select_dtypes(include=[np.number]).columns
df[numeric_cols] = (df[numeric_cols] - df[numeric_cols].min()) / (df[numeric_cols].max() - df[numeric_cols].min())

# 9. Save the cleaned dataset
df.to_csv(r"C:\Users\mysur\OneDrive\Desktop\panda data\cleaned_diabetes_dataset.csv", index=False)

# 10. Load cleaned CSV
csv_path = r"C:\Users\mysur\OneDrive\Desktop\panda data\cleaned_diabetes_dataset.csv"
df = pd.read_csv(csv_path)

# 11. Set up connection to SQL Server (Update with your actual credentials)
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=TEJU;'
    'DATABASE=diabetes_db;'
    'UID=sa;'
    'PWD=Kasmo@123'
)
cursor = conn.cursor()

# 12. Create table if it doesn't exist
# Adjust column names and types based on your dataset
create_table_sql = """
CREATE TABLE diabetes_data (
    sl_no FLOAT,
    Age FLOAT,
    Sex FLOAT,
    Ethnicity FLOAT,
    BMI FLOAT,
    Waist_Circumference FLOAT,
    Fasting_Blood_Glucose FLOAT,
    HbA1c FLOAT,
    Blood_Pressure_Systolic FLOAT,
    Blood_Pressure_Diastolic FLOAT,
    Cholesterol_Total FLOAT,
    Cholesterol_HDL FLOAT,
    Cholesterol_LDL FLOAT,
    GGT FLOAT,
    Serum_Urate FLOAT,
    Physical_Activity_Level FLOAT,
    Dietary_Intake_Calories FLOAT,
    Alcohol_Consumption FLOAT,
    Smoking_Status FLOAT,
    Family_History_of_Diabetes FLOAT,
    Previous_Gestational_Diabetes FLOAT
)
"""

try:
    cursor.execute(create_table_sql)
    conn.commit()
except:
    print("Table may already exist, skipping creation.")

# 13. Dynamically generate column list and placeholders
columns = list(df.columns)
col_str = ", ".join(columns)
placeholders = ", ".join(["?"] * len(columns))

for _, row in df.iterrows():
    cursor.execute(f"""
        INSERT INTO diabetes_data ({col_str}) VALUES ({placeholders})
    """, tuple(row))

# 14. Commit and close
conn.commit()
cursor.close()
conn.close()
