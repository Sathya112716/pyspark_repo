
**Question 1:**

1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data 
•create_purchase_data_df: 
Creates a DataFrame containing purchase records with columns for customer IDs and product models by defining the schema
•create_product_data_df: 
Creates a DataFrame containing product information with a column for product models by defining the schema

2.Find the customers who have bought only iphone13 
•find_customers_with_only_iphone13: Finds customers who bought only the "iphone13" product model so here we can use filter to display the customer.

3.Find customers who upgraded from product iphone13 to product iphone14 
•find_customers_upgraded_to_iphone14: 
oFinds customers who upgraded from the "iphone13" to the "iphone14" product model. 
oHere first we can filter who bought only iphone 13 and iphone 14.
oAnd we can use inner join method so it will display the customer who bought both iphone 13 and iphone 14.

4.Find customers who have bought all models in the new Product Data 
•find_customers_bought_all_products: 
oFinds customers who bought all available products listed in the product data DataFrame.
oIn this we apply aggregate function countDistinct on product models and also groupBy on customer.
oSo,on each customer it will count the product_models and show the customer who bought all products.


**Question 2:**
1.Create a Dataframe as credit_card_df with different read methods 
•create_credit_card_dataframe: 
Creates a DataFrame containing credit card numbers.

2.print number of partitions 
•get_num_partitions:
Returns the number of partitions in a DataFrame.

3.Increase the partition size to 5 
•increase_partitions: 
Increases the number of partitions in a DataFrame to the specified value we can use repartition function.

4.Decrease the partition size back to its original partition size 
•decrease_partitions: 
Decreases the number of partitions in a DataFrame to the default number of partitions by coalesce function.

5.Create a UDF to print only the last 4 digits marking the remaining digits as * 
Eg: ************4567 
6.output should have 2 columns as card_number, masked_card_number(with output of question 2)
•mask_credit_card_numbers: 
oMasks the credit card numbers in a DataFrame by replacing all but the last four digits with asterisks .
oHere I used the udf function by creating the function.


**Question 3:**
1.Create a Data Frame with custom schema creation by using Struct Type and Struct Field 
•create_custom_dataframe:
Creates a DataFrame containing user activity logs with columns for log ID, user ID, action, and timestamp.

2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function 
•rename_columns: 
Renames the columns in a DataFrame to standardize column names for further processing.

•convert_to_timestamp: 
Converts the timestamp column in a DataFrame to the TimestampType for timestamp-based operations here covert string date functions by using cast method.

3.Write a query to calculate the number of actions performed by each user in the last 7 days 
•count_actions_last_7_days: C
oCounts the number of user actions performed in the last 7 days based on the timestamp column
oIt extracts the date from time stamp and checks if the difference in days is less than or equal to 7, meaning the date falls within the last 7 days.

4.Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
•convert_to_login_date
Converts the timestamp column in a DataFrame to the DateType to extract login dates for analysis.


**Question 4:**
1.Read JSON file provided in the attachment using the dynamic function
•Reading JSON File: 
Reads a JSON file into a DataFrame.

2.flatten the data frame which is a custom schema
•Flattening DataFrame:
 Flattens a nested DataFrame structure by exploding arrays and struct columns.

3.find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)
•Count Before and After Flatten: 
Prints the record count of a DataFrame before and after flattening.

4.Differentiate the difference using explode, explode outer, posexplode functions
•Explode vs Explode Outer vs PosExplode:
 Demonstrates the differences between explode, explode outer, and posexplode functions on arrays.

5.Filter the id which is equal to 0001 
•Filtering Employees with ID: 
Filters a DataFrame to retain only rows with a specific employee ID.

6.convert the column names from camel case to snake case
•Converting Column Names: 
Converts column names from camel case to snake case.

7.Add a new column named load_date with the current date
•Adding Load Date:
 Adds a new column named "load_date" with the current date to a DataFrame.

8.create 3 new columns as year, month, and day from the load_date column
•Extracting Year, Month, Day: Creates new columns for year, month, and day from a "load_date" column in a DataFrame.


****Question5:****
1.create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way 
•Creating DataFrames:
 Generates DataFrames for employee, department, and country data with specified schemas.

2.Find avg salary of each department
•Average Salary per Department: 
Computes the average salary for each department.

3.Find the employee’s name and department name whose name starts with ‘m’
•Filter Employees Starting with 'M':
 Filters employees whose names start with the letter 'M' and selects their names and departments.

4.Create another new column in  employee_df as a bonus by multiplying employee salary *2
•Adding Bonus Column: 
Calculates a bonus column based on the salary for each employee.

5.Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)
•Reordering Columns: 
Reorders the columns of the employee DataFrame.

6.Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way
•Joining DataFrames:
 Joins the employee DataFrame with the department DataFrame using the specified join type.

7.Derive a new data frame with country_name instead of State in employee_df  
Eg(11,“james”,”D101”,”newyork”,8900,32)
•Replacing State with Country:
 Replaces the state column in the employee DataFrame with the corresponding country name from the country DataFrame.

8.convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date
•Converting Column Names to Lowercase: 
Converts column names in the employee DataFrame to lowercase and adds a new column named "load_date" with the current date.



