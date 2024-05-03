# Data-Analysis-using-Spark
This project focuses on mastering Spark SQL, a powerful component of Apache Spark that allows you to work with structured data using SQL-like queries. You will create a DataFrame from a CSV file, define a schema for the data, and leverage Spark SQL to perform transformations and actions on the data.

#Scenario

You have been tasked by the HR department of a company to create a data pipeline that can take in employee data in a CSV format. Your responsibilities include analyzing the data, applying any required transformations, and facilitating the extraction of valuable insights from the processed data.

Given your role as a data engineer, you've been requested to leverage Apache Spark components to accomplish the tasks.

Skills Network Labs (SN Labs) is a virtual lab environment used in this course. Upon clicking the "Launch App" button below, your Username and Email will be passed to SN Labs and will be used in strict accordance with IBM Skills Network Privacy policy, such as for communicating important information to enhance your learning experience.

In this project, you will not be provided with hints or solutions. You will create a DataFrame by loading data from a CSV file and apply transformations and actions using Spark SQL. This needs to be achieved by performing the following tasks:

- Task 1: Generate DataFrame from CSV data.
- Task 2: Define a schema for the data.
- Task 3: Display schema of DataFrame.
- Task 4: Create a temporary view.
- Task 5: Execute an SQL query.
- Task 6: Calculate Average Salary by Department.
- Task 7: Filter and Display IT Department Employees.
- Task 8: Add 10% Bonus to Salaries.
- Task 9: Find Maximum Salary by Age.
- Task 10: Self-Join on Employee Data.
- Task 11: Calculate Average Employee Age.
- Task 12: Calculate Total Salary by Department.
- Task 13: Sort Data by Age and Salary.
- Task 14: Count Employees in Each Department.
- Task 15: Filter Employees with the letter o in the Name.
  

Note: the printing result please refer to the attachment of Data-Analysis-using-Spark PDF file.


#Prerequisites 
1. For this lab assignment, you will be using Python and Spark (PySpark). Therefore, it's essential to make sure that the following libraries are installed in your lab environment or within Skills Network (SN) Labs
In [1]:
<!-- Installing required packages   -->
!pip install pyspark  findspark wget

import findspark
findspark.init()

<!-- PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the SparkContext.  --> 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

<!-- Creating a SparkContext object-->

sc = SparkContext.getOrCreate()

 <!--Creating a SparkSession-->
 
spark = SparkSession \
 .builder \
 .appName("Python Spark DataFrames basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()

2. Download the CSV data.

<!-- Download the CSV data first into a local `employees.csv` file-->
import wget
wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwor



#Tasks 
Task 1: Generate a Spark DataFrame from the CSV data Read data from the provided CSV file, employees.csv and import it into a Spark DataFrame variable named
employees_df .

<!-- Read data from the "emp" CSV file and import it into a DataFrame variable named "employees_df"-->  

employees_df = spark.read.csv("employees.csv", header=True, inferSchema=True)
 
Task 2: Define a schema for the data

Construct a schema for the input data and then utilize the defined schema to read the CSV file to create a DataFrame named
employees_df .

<!-- Define a Schema for the input data and read the file using the user-defined Schema-->

employees_df = spark.read.format("csv") \
 .option("header", "true") \
 .option("inferSchema", "true") \
 .load("employees.csv")
 
 Task 3: Display schema of DataFrame Display the schema of the employees_df DataFrame, showing all columns and their respective data types.

<!--Display all columns of the DataFrame, along with their respective data types-->
employees_df.printSchema()

Task 4: Create a temporary view Create a temporary view named employees for the employees_df DataFrame, enabling Spark SQL queries on the data.

<!--  Create a temporary view named "employees" for the DataFrame -->

employees_df.createTempView("employees")

Task 5: Execute an SQL query Compose and execute an SQL query to fetch the records from the employees view where the age of employees exceeds 30. Then, display the result of the SQL query, showcasing the filtered records.

<!-- SQL query to fetch solely the records from the View where the age exceeds 30-->

spark.sql("SELECT * FROM employees WHERE age > 30").show()

Task 6: Calculate Average Salary by Department Compose an SQL query to retrieve the average salary of employees grouped by department. Display the result.

<!-- SQL query to calculate the average salary of employees grouped by department-->

spark.sql("SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department").show()

Task 7: Filter and Display IT Department Employees Apply a filter on the employees_df DataFrame to select records where the department is 'IT' . Display the filtered DataFrame.

<!-- Apply a filter to select records where the department is 'IT'-->

employees_df.filter(employees_df['department'] == 'IT').show()

Task 8: Add 10% Bonus to Salaries Perform a transformation to add a new column named "SalaryAfterBonus" to the DataFrame. Calculate the new salary by adding a 10% bonus to each employee's salary.

from pyspark.sql.functions import col

<!-- Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary-->
employees_df = employees_df.withColumn("SalaryAfterBonus", col("salary") * 1.1)
employees_df.show()

Task 9: Find Maximum Salary by Age Group the data by age and calculate the maximum salary for each age group. Display the result.

from pyspark.sql.functions import max
<!--Group data by age and calculate the maximum salary for each age group-->

employees_df.groupBy('age').agg(max('salary').alias('max_salary')).show()

Task 10: Self-Join on Employee Data Join the "employees_df" DataFrame with itself based on the "Emp_No" column. Display the result.

<!-- Join the DataFrame with itself based on the "Emp_No" column-->

employees_df.join(employees_df, 'Emp_No', how='inner').show()

Task 11: Calculate Average Employee Age Calculate the average age of employees using the built-in aggregation function. Display the result.

<!-- Calculate the average age of employees-->

from pyspark.sql.functions import avg
employees_df.agg(avg('age').alias('average_age')).show()

Task 12: Calculate Total Salary by Department Calculate the total salary for each department using the built-in aggregation function. Display the result.

<!-- Calculate the total salary for each department. Hint - User GroupBy and Aggregate functions-->

from pyspark.sql.functions import sum
employees_df.groupBy('department').agg(sum('salary').alias('total_salary')).show()

Task 13: Sort Data by Age and Salary Apply a transformation to sort the DataFrame by age in ascending order and then by salary in descending order. Display the sorted DataFrame.

<!-- Sort the DataFrame by age in ascending order and then by salary in descending order-->

employees_df.orderBy(employees_df["age"].asc(), employees_df["salary"].desc()).show()

Task 14: Count Employees in Each Department Calculate the number of employees in each department. Display the result.

from pyspark.sql.functions import count

<!-- Calculate the number of employees in each department-->

employees_df.groupBy('department').agg(count('Emp_Name').alias('numbers_employees')).show()

Task 15: Filter Employees with the letter o in the Name Apply a filter to select records where the employee's name contains the letter 'o' . Display the filtered DataFrame.

<!--Apply a filter to select records where the employee's name contains the letter 'o'-->

employees_df.filter('Emp_Name like "%o%"').show()
