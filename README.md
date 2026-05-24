# 🏥 Patient Data Pipeline using AWS & Databricks (PySpark)

---

## 📌 Project Overview

This project demonstrates an **end-to-end data engineering workflow** where patient data is:

* Uploaded to AWS S3 using Python (boto3)
* Extracted using AWS Lambda
* Processed in Databricks using PySpark
* Cleaned and transformed for analysis

---

## 🚀 Architecture / Workflow

```
Local (VS Code - boto3)
        ↓
AWS S3 (ZIP Upload)
        ↓
AWS Lambda (Extract ZIP)
        ↓
S3 (Processed CSV Files)
        ↓
Databricks (PySpark DataFrame)
        ↓
Data Cleaning & Transformation
```

---

## 📂 Step 1: S3 Bucket Creation & Upload

* Created S3 bucket using boto3
* Uploaded ZIP file containing patient dataset

### 📸 Screenshot

![S3 Upload](image/s3.png)

---

## ⚙️ Step 2: Data Extraction using AWS Lambda

* Lambda function triggered
* Extracted ZIP file
* Stored CSV files in `extracted/` folder

### 💻 Lambda Function Code

```python
import boto3
import zipfile
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):

    print("Event:", event)   # 👈 debug

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print("File name:", key)  # 👈 debug

    # safer check
    if '.zip' not in key.lower():
        print("Not a zip file")
        return

    zip_path = '/tmp/file.zip'
    extract_path = '/tmp/extracted'

    s3.download_file(bucket, key, zip_path)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_path)

    for file in os.listdir(extract_path):
        file_path = os.path.join(extract_path, file)

        if os.path.isfile(file_path):
            s3.upload_file(file_path, bucket, "extracted/" + file)

    print("Unzip done")

    return {
        'statusCode': 200,
        'body': 'Unzip done'
    }
```

---

## 📊 Step 3: Data Ingestion in Databricks

* Used direct S3 path (`s3a://`) to load CSV
* Loaded CSV into PySpark DataFrame

### 📸 Screenshot

![Databricks Load](image/databricks_load.png)

---

## 🧹 Data Cleaning & Transformations

---

### 🔹 1. Standardizing Names

* Converted `first_name` to title case using `initcap`

### 📸

![Uppercase Name](image/name_upper.png)

---

### 🔹 2. Handling Missing Values

* `contact_phone` → 000, 999, "" → NULL
* `pincode` → 000, 999, "" → NULL

### 📸

![Missing Values - Contact](image/missing_contact.png)

![Missing Values - Pincode](image/missing_pincode.png)

---

### 🔹 3. Date Validation

* Checked if `date_of_birth` > current date
* Cleaned null and empty `date_of_birth` values ✅

### 📸

![Date of Birth](image/date_of_birth.png)

---

### 🔹 4. Age Calculation

* Created `age` column from DOB

### 📸

![Age Calculation](image/age.png)

---

### 🔹 4. Gender Standardization

* `male`, `m` → `m`
* `female`, `f` → `f`
* Others → `o`

### 📸

![Gender](image/gender.png)

---

### 🔹 5. Blood Type Standardization

* Converted values like:
  * `a+`, `A POSITIVE` → `A+`
  * `b-`, `B NEGATIVE` → `B-`
  * `AB POSITIVE` → `AB+`, etc.

### 📸

![Blood Type](image/blood.png)

---

### 🔹 6. Chronic Disease Count

* Created column: `chronic_disease_count`
* Counted diseases using `|` delimiter via `split` and `size`

### 📸

![Disease Count](image/disease.png)

---

### 🔹 7. Policy Status

* Created column: `policy_active`

### Logic:

* `True` → policy_end_date ≥ current_date
* `False` → expired

### 📸

![Policy](image/policy.png)

---

## 🛠️ Technologies Used

* Python (boto3)
* AWS S3
* AWS Lambda
* Databricks (Serverless)
* PySpark

---

## ⚠️ Challenges Faced

* Serverless limitations (no direct S3 access)
* Data type mismatches (int vs string)
* Handling inconsistent real-world data
* Working with presigned URLs

---

## ✅ Key Learnings

* End-to-end data pipeline design
* Data cleaning using PySpark
* Handling nulls and invalid data
* Working with AWS services

---

## 🔮 Future Improvements

* Use Unity Catalog for secure S3 access
* Convert CSV to Parquet format
* Automate pipeline (trigger-based)
* Add data validation rules

---

## 🙌 Conclusion

This project demonstrates practical knowledge of:

* Data Engineering workflows
* Cloud integration (AWS + Databricks)
* Data cleaning and transformation
* Real-world dataset handling

---

## 📌 Image Reference

Place your screenshots in an `image/` folder alongside this README:

```
image/
 ├── s3.png
 ├── databricks_load.png
 ├── name_upper.png
 ├── missing_contact.png
 ├── missing_pincode.png
 ├── age.png
 ├── gender.png
 ├── blood.png
 ├── disease.png
 └── policy.png
```

---

# ⭐ Thank You
