# Synthetic Data For Testing Big Data Workflows

This guide provides an overview of generating and using synthetic data for testing purposes.

## 1. Introduction to Synthetic Data Generation


**What is Synthetic Data?** 

* **Definition:** "Synthetic data are microdata records created to improve data utility while preventing disclosure of confidential respondent information. Synthetic data is created by statistically modelling original data and then using those models to generate new data values that reproduce the original data’s statistical properties. Users are unable to identify the information of the entities that provided the original data.” - (source: [US Census Bureau](https://definedterm.com/synthetic_data/80955))   

* **Importance in Testing:** Using synthetic data allows developers to test algorithms, conduct performance analysis, and ensure code integrity without worrying about the resource demands or privacy concerns of production datasets.  

* **Use Cases:**

    * provision of microdata to users  
    * testing systems  
    * developing systems or methods in non-secure environments  
    * obtaining non-disclosive data from data suppliers  
    * teaching – a useful way to promote the use of ONS data sources   


**Why Use Synthetic Data in Big Data Workflows?**

* **Cost Efficiency:** Synthetic data can be generated on-demand and used to reduce the usage of cloud resources (memory, CPU, storage).  

* **No Privacy Concerns:** Generating synthetic data eliminates the privacy risks associated with using sensitive real-world data.  

* **Resource Optimisation:** By generating datasets of the required size, synthetic data can be scaled without impacting performance. 


## 2. Methods for Generating Synthetic Data

This section will intoduce various Python and R libraries used to generate synthetic data.


### 2.1 Faker (Python)

* **Overview:** `Faker` is a Python package that generates fake data such as names, addresses, emails, dates, credit card numbers, and more.

* **Key Features:** Randomised generation, locale support, wide range of data types.

* **Example Usage:**
````{tabs}
```{code-tab} py
from faker import Faker
```
````

### Create a Faker instance
````{tabs}
```{code-tab} py
fake = Faker()
```
````

### Generate synthetic data
````{tabs}
```{code-tab} py
fake.name()  # Generates a random name
fake.address()  # Generates a random address
fake.email()  # Generates a random email
```
````
## 2.2. fakeR (R)
* **Overview:** `fakeR` is an `R` package for generating synthetic data. It supports generating fake names, addresses, phone numbers, dates, and more.

* **Key Features:** Easy integration with `R`, flexible data generation, wide variety of fake data types.

* **Example Usage:**
````{tabs}
```{code-tab} r
library(fakeR)
```
````

### Generate synthetic data

````{tabs}
```{code-tab} r
fake_name()  # Random name
fake_address()  # Random address
fake_phone_number()  # Random phone number
```
````

## 2.3. Other Python Libraries
* **mimesis:** An alternative to `Faker`, offering similar functionality for generating fake data.  
* **synthetic:** A Python package focused on generating datasets for machine learning tasks.
* **Example Usage:**
````{tabs}
```{code-tab} py
from mimesis import Person

person = Person()
```
````

### Generate synthetic data
````{tabs}
```{code-tab} py
person.full_name()
person.email()
```
````

## 2.4. Other R Libraries
* **random:** An R package for generating random data (often used in statistics).  
* **simputation:** Another R package for generating synthetic data for missing values.  

**Example Usage:**
````{tabs}
```{code-tab} r
library(random)
randomInteger(min = 1, max = 100)  # Random integer
```
````


# 3. Using Synthetic Data with Big Data Frameworks

## 3.1. Using Synthetic Data with PySpark
`PySpark` is the Python API for Apache Spark, a distributed computing framework commonly used for big data workflows.

**Goal:** Demonstrate how to create synthetic data in PySpark and use it within Spark DataFrames.

**Example Usage:**
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from faker import Faker

# Initialize Spark session
spark = SparkSession.builder.appName("Synthetic Data Example").getOrCreate()


# Create synthetic data with Faker
fake = Faker()

n_rows = 100
data = [(fake.name(), fake.address(), fake.email()) for _ in range(n_rows)]


# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Address", "Email"])

# Show the DataFrame
df.show()
```
````
**Application to Big Data:**
This example can be extended to generate large datasets (millions of records) that can be processed in parallel using PySpark.

## 3.2. Using Synthetic Data with SparklyR
`SparklyR` is an `R` interface to Apache Spark, enabling `R` users to perform distributed data analysis.

**Goal:** Demonstrate how to generate and manipulate synthetic data in `SparklyR`.  

**Example Usage:**  
````{tabs}
```{code-tab} r
library(sparklyr)
library(dplyr)
library(fakeR)

# Connect to Spark
sc <- spark_connect(master = "local")

# Generate synthetic data
n_rows <- 100
data <- data.frame(
  Name = replicate(n_rows, fake_name()),
  Address = replicate(n_rows, fake_address()),
  Email = replicate(n_rows, fake_email())
)

# Copy data to Spark
spark_data <- copy_to(sc, data, overwrite = TRUE)

# Show the DataFrame
spark_data %>% head()
```
````

# 5. **Generate Synthetic Data with More Features**

````{tabs}
```{code-tab} py
# Import Libraries & Setup

import pandas as pd
from faker import Faker
import random
import numpy as np
from pyspark.sql import SparkSession
```
````

**Generate Synthetic Data with Additional Features**

We will generate synthetic data with the following additional features:
1. Mode of Transportation (Car, Public Transport, Walking, etc.)  
2. Highest Education (High School, Bachelor's, Master's, PhD, etc.)  
3. Marital Status (Single, Married, Divorced, Widowed)  
4. Favorite High Street Supermarket (Tesco, Sainsbury's, Asda, etc.)  
5. Pet Ownership (Yes, No - with type of pet if Yes)  

We will also simulate some missing data in the dataset, which is commonly encountered in real-world scenarios.


````{tabs}
```{code-tab} py
# Import Libraries & Setup
# Initialize Faker instance with UK locale
fake = Faker('en_GB')

# Define the number of rows
n_samples = 1000  # You can change this variable to generate more or fewer rows

# Create lists of categories for features
mode_of_transport = ['Car', 'Public Transport', 'Walking', 'Cycling', 'Taxi']
education_levels = ['Primary', 'High School', 'Vocational', 'Bachelor\'s', 'Master\'s', 'PhD']
marital_status = ['Single', 'Married', 'Divorced', 'Widowed']
supermarkets = ['Tesco', 'Sainsbury\'s', 'Asda', 'Morrisons', 'Waitrose']
pets = ['Dog', 'Cat', 'None']

# Generate synthetic data with the new features
data = []
for _ in range(n_samples):
    name = fake.name()
    address = fake.address()
    postcode = fake.postcode()
    city = fake.city()
    email = fake.email()
    transport = random.choice(mode_of_transport)
    education = random.choice(education_levels)
    marital = random.choice(marital_status)
    supermarket = random.choice(supermarkets)
    pet = random.choice(pets)
    
    # Simulating missing data by randomly omitting some features
    if random.random() < 0.1:  # 10% chance to have missing data for 'Mode of Transport'
        transport = None
    if random.random() < 0.1:  # 10% chance to have missing data for 'Education'
        education = None
    if random.random() < 0.1:  # 10% chance to have missing data for 'Marital Status'
        marital = None
    if random.random() < 0.1:  # 10% chance to have missing data for 'Supermarket'
        supermarket = None
    if random.random() < 0.1:  # 10% chance to have missing data for 'Pet'
        pet = None

    data.append([name, address, postcode, city, email, transport, education, marital, supermarket, pet])

# Convert the data to a DataFrame
columns = ['Name', 'Address', 'Postcode', 'City', 'Email', 'Mode_of_Transport', 'Education', 'Marital_Status', 'Supermarket', 'Pet']
synthetic_df = pd.DataFrame(data, columns=columns)

# Display the first few rows
synthetic_df.head()
```
````

**Integrate with PySpark for Big Data Workflow**

Now that we have a robust dataset with additional features and some missing data, let's see how to integrate this with PySpark, which is commonly used in big data workflows.


````{tabs}
```{code-tab} py
# Initialize Spark session
spark = SparkSession.builder.appName("SyntheticDataForBigData").getOrCreate()

# Convert the synthetic DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(synthetic_df)

# Show the Spark DataFrame
spark_df.show(5)
```
````

**Working with Missing Data in PySpark**


Handling Missing Data

In real-world datasets, missing values are common. Let's demonstrate how we can handle missing data in PySpark by performing:
1. Dropping rows with missing data
2. Filling missing values with defaults or statistical measures


````{tabs}
```{code-tab} py
# Drop rows with any missing values
clean_df = spark_df.na.drop()

# Show the DataFrame after dropping missing rows
clean_df.show(5)

# Fill missing values with default values (e.g., 'Unknown' for missing Mode_of_Transport)
filled_df = spark_df.fillna({'Mode_of_Transport': 'Unknown', 'Education': 'Unknown', 'Marital_Status': 'Unknown'})

# Show the DataFrame after filling missing values
filled_df.show(5)
```
````

# Conclusion
**Summary:** Generating synthetic data is a crucial tool for testing algorithms, saving resources, and maintaining privacy. `Python` and `R` offer various packages to generate synthetic data, and big data frameworks like `PySpark` and `SparklyR` can help handle large datasets efficiently.

**Recommendation:** Use synthetic data in development and testing environments to improve efficiency, save costs, and ensure compliance with privacy regulations.

# References and Further Reading
* Faker Documentation (Python)
* fakeR Documentation (R)
* PySpark Documentation
* SparklyR Documentation