## **Question_1:**

1.Create DataFrame as purchase_data_df,  product_data_df with custom schema with the below data 

2.Find the customers who have bought only iphone13**
        --perform the filter function on the particular column
        
3.Find customers who upgraded from product iphone13 to product iphone14** 
        --perform the filter function and use join for the upgraded product by customer
        
**4.Find customers who have bought all models in the new Product Data** 
--First list all the product without duplicates
--Perform the aggregrate function 
--Then display the customer who bought all models 


## Question_2:

**1.Create a Dataframe as credit_card_df with different read methods** 
a.By creating a dataframe
b.By using schema
c.from a list of rows
**2. print number of partitions** 
--For the created dataframe print the number of partitions
**3. Increase the partition size to 5** 
--To increase or decrease there is a method called repartition
**4. Decrease the partition size back to its original partition size** 
--This is done by coalesce method
**5.Create a UDF to print only the last 4 digits marking the remaining digits as *** 

Eg: ************4567 
--Define the function and perform the required condition
**6.output should have 2 columns as card_number, masked_card_number(with output of question 2)** 
--print the output here
