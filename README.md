# Data Processing Tasks

## Question 1:

### 1. Create DataFrames with Custom Schema:
- **create_purchase_data_df:**  
  - Creates a DataFrame containing purchase records with columns for customer IDs and product models by defining the schema.
  
- **create_product_data_df:**  
  - Creates a DataFrame containing product information with a column for product models by defining the schema.

### 2. Find Customers who Bought Only iPhone13:
- **find_customers_with_only_iphone13:**  
  - Finds customers who bought only the "iPhone13" product model by using filtering.

### 3. Find Customers who Upgraded from iPhone13 to iPhone14:
- **find_customers_upgraded_to_iphone14:**  
  - Finds customers who upgraded from the "iPhone13" to the "iPhone14" product model.
  - Filters customers who bought both iPhone 13 and iPhone 14 using an inner join method.

### 4. Find Customers who Bought All Models in the New Product Data:
- **find_customers_bought_all_products:**  
  - Finds customers who bought all available products listed in the product data DataFrame.
  - Applies an aggregate function `countDistinct` on product models and `groupBy` on customer to count the product models and identify customers who bought all products.

## Question 2:

### 1. Create DataFrame with Different Read Methods:
- **create_credit_card_dataframe:**  
  - Creates a DataFrame containing credit card numbers.

### 2. Print Number of Partitions:
- **get_num_partitions:**  
  - Returns the number of partitions in a DataFrame.

### 3. Increase Partition Size to 5:
- **increase_partitions:**  
  - Increases the number of partitions in a DataFrame to the specified value using the `repartition` function.

### 4. Decrease Partition Size back to Original Partition Size:
- **decrease_partitions:**  
  - Decreases the number of partitions in a DataFrame to the default number of partitions using the `coalesce` function.

### 5. Create a UDF to Mask Credit Card Numbers:
- **mask_credit_card_numbers:**  
  - Masks the credit card numbers in a DataFrame by replacing all but the last four digits with asterisks using a User Defined Function (UDF).

## Question 3:

### 1. Create DataFrame with Custom Schema using Struct Type and Struct Field:
- **create_custom_dataframe:**  
  - Creates a DataFrame containing user activity logs with columns for log ID, user ID, action, and timestamp.

### 2. Rename Columns and Convert Timestamp:
- **rename_columns:**  
  - Renames the columns in a DataFrame to standardize column names for further processing.
  
- **convert_to_timestamp:**  
  - Converts the timestamp column in a DataFrame to the TimestampType for timestamp-based operations by casting string dates.

### 3. Count Actions performed by Each User in the Last 7 Days:
- **count_actions_last_7_days:**  
  - Counts the number of user actions performed in the last 7 days based on the timestamp column.

### 4. Convert Timestamp Column to Login Date Column:
- **convert_to_login_date:**  
  - Converts the timestamp column in a DataFrame to the DateType to extract login dates for analysis.

## Question 4:

### 1. Read JSON File:
- **Reading JSON File:**  
  - Reads a JSON file into a DataFrame.

### 2. Flatten the DataFrame with Custom Schema:
- **Flattening DataFrame:**  
  - Flattens a nested DataFrame structure by exploding arrays and struct columns.

### 3. Count Records Before and After Flatten:
- **Count Before and After Flatten:**  
  - Prints the record count of a DataFrame before and after flattening.

### 4. Differentiate Functions: Explode vs Explode Outer vs PosExplode:
- **Explode vs Explode Outer vs PosExplode:**  
  - Demonstrates the differences between `explode`, `explode outer`, and `posexplode` functions on arrays.

### 5. Filter ID Equal to '0001':
- **Filtering Employees with ID:**  
  - Filters a DataFrame to retain only rows with a specific employee ID.

### 6. Convert Column Names from Camel Case to Snake Case:
- **Converting Column Names:**  
  - Converts column names from camel case to snake case.

### 7. Add Load Date Column:
- **Adding Load Date:**  
  - Adds a new column named "load_date" with the current date to a DataFrame.

### 8. Create Year, Month, and Day Columns from Load Date:
- **Extracting Year, Month, Day:**  
  - Creates new columns for year, month, and day from a "load_date" column in a DataFrame.

## Question 5:

### 1. Create DataFrames with Custom Schema:
- **Creating DataFrames:**  
  - Generates DataFrames for employee, department, and country data with specified schemas.

### 2. Find Average Salary per Department:
- **Average Salary per Department:**  
  - Computes the average salary for each department.

### 3. Filter Employees Whose Name Starts with 'M':
- **Filter Employees Starting with 'M':**  
  - Filters employees whose names start with the letter 'M' and selects their names and departments.

### 4. Add Bonus Column:
- **Adding Bonus Column:**  
  - Calculates a bonus column based on the salary for each employee.

### 5. Reorder Columns in Employee DataFrame:
- **Reordering Columns:**  
  - Reorders the columns of the employee DataFrame.

### 6. Perform Inner, Left, and Right Joins Dynamically:
- **Joining DataFrames:**  
  - Joins the employee DataFrame with the department DataFrame using the specified join type.

### 7. Replace State with Country Name:
- **Replacing State with Country:**  
  - Replaces the state column in the employee DataFrame with the corresponding country name from the country DataFrame.

### 8. Convert Column Names to Lowercase and Add Load Date Column:
- **Converting Column Names to Lowercase:**  
  - Converts column names in the employee DataFrame to lowercase and adds a new column named "load_date" with the current date.
