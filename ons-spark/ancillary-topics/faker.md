## Faker: Synthetic Data in Python

### 1. Introduction to Faker

`Faker` is a Python package that generates fake data such as names, addresses, emails, dates, credit card numbers, and more. It is similar to [`Mimesis`](../ancillary-topics/mimesis.html)

**Key Features:** Randomised generation, locale support, wide range of data types.

### 2. Install required libraries
To start, we will need the `Faker` library to generate the dummy data. Additionally, we will use PySpark for working with Spark DataFrames in Python.

For Python, use:
````{tabs}
```{code-tab} py
pip install Faker
```
````

````{tabs}

```{code-tab} plaintext Python Output
Looking in indexes: https://njobud:****@onsart-01.ons.statistics.gov.uk/artifactory/api/pypi/yr-python/simple
/usr/share/python-wheels/urllib3-1.25.8-py2.py3-none-any.whl/urllib3/connectionpool.py:1004: InsecureRequestWarning: Unverified HTTPS request is being made to host 'onsart-01.ons.statistics.gov.uk'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
Collecting Faker
/usr/share/python-wheels/urllib3-1.25.8-py2.py3-none-any.whl/urllib3/connectionpool.py:1004: InsecureRequestWarning: Unverified HTTPS request is being made to host 'onsart-01.ons.statistics.gov.uk'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
  Downloading https://onsart-01.ons.statistics.gov.uk/artifactory/api/pypi/yr-python/packages/packages/96/18/f70c67c9d3a71f1749faa027dad3cd626c728df5682458091d73c69ed9a9/Faker-35.2.2-py3-none-any.whl (1.9 MB)
[K     |████████████████████████████████| 1.9 MB 5.7 MB/s eta 0:00:01
[?25hRequirement already satisfied: python-dateutil>=2.4 in /runtime-addons/cmladdon-2.0.47-b365/opt/cmladdons/python/site-packages (from Faker) (2.9.0.post0)
Requirement already satisfied: typing-extensions in /runtime-addons/cmladdon-2.0.47-b365/opt/cmladdons/python/site-packages (from Faker) (4.10.0)
Requirement already satisfied: six>=1.5 in /runtime-addons/cmladdon-2.0.47-b365/opt/cmladdons/python/site-packages (from python-dateutil>=2.4->Faker) (1.17.0)
Installing collected packages: Faker
Successfully installed Faker-35.2.2
Note: you may need to restart the kernel to use updated packages.
```
````
For further details to assist on the setup, please visit the [official documentation](https://faker.readthedocs.io/en/master/).

**Check the version of the `Faker` package installed in your environment**
````{tabs}
```{code-tab} py
import pkg_resources
faker_version = pkg_resources.get_distribution("Faker").version
print(f"faker_version: {faker_version}")
```
````

````{tabs}

```{code-tab} plaintext Python Output
faker_version: 35.2.2
```
````
**Set up a global random seed for reproducibility**

You can set a global seed for all data providers and use it without explicitly passing it to each provider:
````{tabs}
```{code-tab} py
from faker import Faker
fake = Faker()
fake.seed_instance(42)
```
````
### 3. Generate a first fake data
````{tabs}
```{code-tab} py
# Generate sample personal data
print("Name:", fake.name())
print("Email:", fake.email())
print("Address:", (fake.address()).replace("\n", ", ")) # the address method has new line characters (\n), hence, replace with ", " to have all in one line
print("Phone:", fake.phone_number())
print("Birthday:", fake.date_of_birth(minimum_age=18, maximum_age=90))
```
````

````{tabs}

```{code-tab} plaintext Python Output
Name: Allison Hill
Email: donaldgarcia@example.net
Address: 600 Jeffery Parkways, New Jamesside, MT 29394
Phone: 394.802.6542x351
Birthday: 1941-05-13
```
````
### 4. International data generation


Faker has a great feature that allows it to generate data tailored to specific locales. Here's how you can create data that reflects various locations:
````{tabs}
```{code-tab} py
# Create localised Faker instances
fake_us = Faker('en_US')
fake_uk = Faker('en_GB')
fake_fr = Faker('fr_FR')
fake_jp = Faker('ja_JP')

print("US:", fake_us.address().replace("\n", ", "))
print("UK:", fake_uk.address().replace("\n", ", "))
print("France:", fake_fr.address().replace("\n", ", "))
print("Japan:", fake_jp.address().replace("\n", ", "))
```
````

````{tabs}

```{code-tab} plaintext Python Output
US: 737 Christopher Springs Suite 170, South Lisa, NM 20713
UK: Studio 8, Thomas ports, Coopertown, M56 9QR
France: 5, rue Alves, 54783 Fournier
Japan: 静岡県袖ケ浦市麹町4丁目7番20号
```
````
Notice the differences in address formats:

* US addresses include state abbreviations and ZIP codes.  
* UK addresses use British postal codes.  
* France addresses follow European conventions.  
* Japan addresses are formatted with the correct characters and local conventions.


For an up-to-date list of supported locales, you can check the [official documentation](https://fakerjs.dev/guide/localization.html#available-locales).

### 5. Faker’s provider architecture


Faker utilises a modular system of "providers," where each provider is responsible for generating a specific type of data. Let's take a closer look at how providers work and the types available:
````{tabs}
```{code-tab} py
from faker import Faker

fake = Faker('en_GB')

print("Available Faker Providers:")
for provider in fake.providers:
    print(f"- {provider}")
```
````

````{tabs}

```{code-tab} plaintext Python Output
Available Faker Providers:
- <faker.providers.user_agent.Provider object at 0x7f8357bb7c40>
- <faker.providers.ssn.en_GB.Provider object at 0x7f8357bb7be0>
- <faker.providers.sbn.Provider object at 0x7f8357bb7b80>
- <faker.providers.python.Provider object at 0x7f8357bb7b20>
- <faker.providers.profile.Provider object at 0x7f8357bb7ac0>
- <faker.providers.phone_number.en_GB.Provider object at 0x7f8357bb7a60>
- <faker.providers.person.en_GB.Provider object at 0x7f8357bb7940>
- <faker.providers.passport.en_US.Provider object at 0x7f8357bb79a0>
- <faker.providers.misc.en_US.Provider object at 0x7f8357bb7880>
- <faker.providers.lorem.la.Provider object at 0x7f8357bb77f0>
- <faker.providers.job.en_US.Provider object at 0x7f8357bb78b0>
- <faker.providers.isbn.en_US.Provider object at 0x7f8357bb76d0>
- <faker.providers.internet.en_GB.Provider object at 0x7f8357bb77c0>
- <faker.providers.geo.en_US.Provider object at 0x7f8357bb7760>
- <faker.providers.file.Provider object at 0x7f8357bb7730>
- <faker.providers.emoji.Provider object at 0x7f8357bb7610>
- <faker.providers.date_time.en_US.Provider object at 0x7f836c1b0bb0>
- <faker.providers.currency.en_US.Provider object at 0x7f836c1b0e20>
- <faker.providers.credit_card.en_US.Provider object at 0x7f836c1b0e80>
- <faker.providers.company.en_US.Provider object at 0x7f836c1b0e50>
- <faker.providers.color.en_US.Provider object at 0x7f836c1a3dc0>
- <faker.providers.barcode.en_US.Provider object at 0x7f836c1e4d00>
- <faker.providers.bank.en_GB.Provider object at 0x7f836c1a32e0>
- <faker.providers.automotive.en_GB.Provider object at 0x7f836c1a3a30>
- <faker.providers.address.en_GB.Provider object at 0x7f836c1a3af0>
```
````
Each item in the list corresponds to a specific provider, such as:

- **User Agent Provider:** Generates browser and device identification strings.  
- **SSN Provider:** Creates valid-format social security numbers (US-specific).  
- **SBN Provider:** Generates 9-digit Standard Book Numbers (used in older systems prior to 1974).  

In addition to these, Faker includes other commonly used providers for generating various types of data:

- **Person Provider:** Generates names, birthdates, and personal details.  
- **Address Provider:** Produces realistic street addresses and postal codes.
- **Internet Provider:** Generates email addresses, domain names, and URLs.

You can explore the full list of available providers and their methods in the official documentation, which offers detailed information and usage examples for each one.

### 6. Best practices for using Faker

When working with Faker, keep the following best practices in mind:

- Select locale-specific providers when generating data tailored to a particular region.
- Combine different providers to generate more realistic and interconnected data.
- Organise your fake data generation to align with the requirements of your application.
- Explore the official documentation to learn about additional provider features and options.

### 7. Building a typical dataset


Now, let's explore how to combine multiple providers to generate rich and interconnected data for more complex structures:
````{tabs}
```{code-tab} py
from faker import Faker
import pandas as pd

fake = Faker('en_GB')
fake.seed_instance(42)

n_rows = 100


def generate_user_profile():
    """ Function to generate a user profile """
    return {
        'name': fake.name(),
        'age': fake.random_int(min=18, max=80),
        'email': fake.email(),
        
        'street': fake.street_address().replace("\n", " "),
        'city': fake.city(),
        'postcode': fake.postcode(),
        
        'job': fake.job(),
        'company': fake.company(),
        
        'username': fake.user_name(),
        'website': fake.url()
    }
 
profiles = [generate_user_profile() for _ in range(n_rows)]
 
df = pd.DataFrame(profiles)

df.head()
```
````

````{tabs}

```{code-tab} plaintext Python Output
                   name  age                        email  \
0      William Jennings   35  francisdavidson@example.org   
1      Benjamin Simpson   42     fisherteresa@example.net   
2    Ricky Lloyd-Duncan   67       wilsonryan@example.com   
3      Dr Jasmine Smith   53   daviesgeoffrey@example.net   
4  Norman Sharp-Stewart   52     scottknowles@example.net   

                    street         city  postcode                  job  \
0         600 Charlie fort  New Joeside  DT79 0GS    Librarian, public   
1    Studio 31 Irene forks    Jasonbury    S4 5GQ     Marine scientist   
2     Flat 13K Kelly parks   Youngmouth   M23 9SY        Oceanographer   
3  Studio 51s Steele alley   Donnaburgh   E4W 2QG  Solicitor, Scotland   
4         89 Arnold plains  Lake Graeme  SG57 1JJ         Bonds trader   

                    company        username                       website  
0             Bryan-Andrews       timothy16     http://butler-gough.info/  
1                Davies Ltd      johnronald  http://www.lamb-scott.co.uk/  
2              Lloyd-Turner          dgreen         http://www.walsh.biz/  
3  Bell, Anderson and Jones  murraymohammad          http://www.shaw.com/  
4              Brown-Naylor   thomashammond          http://www.howe.com/  
```
````
### 8. Using synthetic data with big data frameworks

#### 8.1. Using synthetic data with PySpark

Demonstrate how to create synthetic data in PySpark and use it within Spark DataFrames.
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from faker import Faker
import pandas as pd


# Initialise Spark session
spark = SparkSession.builder.master("local[2]").appName("Synthetic Data Example").getOrCreate()


# Create synthetic data with Faker
fake = Faker('en_GB')
fake.seed_instance(42)

n_rows = 100
data = [(fake.name(), fake.address(), fake.email()) for _ in range(n_rows)]

df_pandas = pd.DataFrame(data, columns=["Name", "Address", "Email"])

# Replace newline characters with commas in the 'Address' column using chaining
df_pandas['Address'] = df_pandas['Address'].str.replace("\n", ", ")

#I'm having problems when using spark.createDataFrame with virtual environment, hence, I have to create a csv file and read it
df_pandas.to_csv('temp_01.csv', index=False)

df_spark = spark.read.csv('temp_01.csv', header=True, inferSchema=True)

df_spark.show(5)
```
````

````{tabs}

```{code-tab} plaintext Python Output
+--------------------+--------------------+--------------------+
|                Name|             Address|               Email|
+--------------------+--------------------+--------------------+
|    William Jennings|2 Sian streets, N...|francescaharrison...|
|     Rosemary Wright|654 Robin track, ...|simpsongemma@exam...|
|         Sean Norton|103 Robinson walk...|  rita19@example.net|
|       Brenda Briggs|Studio 4, Lydia i...|iwilkins@example.org|
|Leonard Powell-Mo...|Flat 32G, Green c...|andrea01@example.net|
+--------------------+--------------------+--------------------+
only showing top 5 rows
```
````
**Application to Big Data:**

This example can be extended to generate large datasets (millions of records) that can be processed in parallel using PySpark.

#### 8.2. Generate synthetic data with more features

We will generate synthetic data with the following additional features:
1. Mode of Transportation (Car, Public Transport, Walking, etc.)  
2. Highest Education (High School, Bachelor's, Master's, PhD, etc.)  
3. Marital Status (Single, Married, Divorced, Widowed)  
4. Favorite High Street Supermarket (Tesco, Sainsbury's, Asda, etc.)  
5. Pet Ownership (Yes, No - with type of pet if Yes)  

We will also simulate some missing data in the dataset, which is commonly encountered in real-world scenarios.
````{tabs}
```{code-tab} py
import pandas as pd
from faker import Faker
from faker.generator import random
import numpy as np
from pyspark.sql import SparkSession


fake = Faker('en_GB')
fake.seed_instance(42)

n_samples = 1000   

# Create lists of categories for features not captured in the Faker library
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
    transport = fake.random_element(mode_of_transport)
    education = fake.random_element(education_levels)
    marital = fake.random_element(marital_status)
    supermarket = fake.random_element(supermarkets)
    pet = fake.random_element(pets)
    
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


columns = ['Name', 'Address', 'Postcode', 'City', 'Email', 'Mode_of_Transport', 'Education', 'Marital_Status', 'Supermarket', 'Pet']
synthetic_df = pd.DataFrame(data, columns=columns)
synthetic_df['Address'] = synthetic_df['Address'].str.replace("\n", ", ")


synthetic_df.head()
```
````

````{tabs}

```{code-tab} plaintext Python Output
                Name                                            Address  \
0   William Jennings                2 Sian streets, New Maryton, E3 8ZA   
1  Dr Josh Pritchard     Studio 16, Lynn hill, Melissaborough, BR0X 4DJ   
2      Leigh Randall              0 Alexander circles, New Guy, W67 4FJ   
3    Lorraine Palmer  Studio 01, Read junctions, West Tracyburgh, AB...   
4         June Sharp                782 Hill rest, Arnoldside, SM3Y 6QT   

   Postcode             City                       Email Mode_of_Transport  \
0   L8G 7YL  Port Samchester         ricky23@example.com               Car   
1    S4 5GQ         Rhysview      johnronald@example.net  Public Transport   
2  NW8M 9RQ     East Natasha          dgreen@example.org  Public Transport   
3  MK9H 5GX  New Brandonfort  griffithslinda@example.com  Public Transport   
4   W1D 1PA       New Gerald    hammondjulia@example.org  Public Transport   

     Education Marital_Status Supermarket   Pet  
0         None        Widowed       Tesco   Cat  
1  High School        Widowed   Morrisons   Cat  
2          PhD        Married        None  None  
3      Primary        Married    Waitrose  None  
4      Primary         Single        Asda   Dog  
```
````
**Integrate with PySpark for Big Data Workflow**

Now that we have a robust dataset with additional features and some missing data, let's see how to integrate this with PySpark, which is commonly used in big data workflows.
````{tabs}
```{code-tab} py

spark = SparkSession.builder.master("local[2]").appName("SyntheticDataForBigData").getOrCreate()

# Convert the synthetic DataFrame to a Spark DataFrame
# Currently, using the createDataFrame on a virtual environment raising error. So, I will convert to a csv file and read the file using spark
# spark_df = spark.createDataFrame(synthetic_df)
synthetic_df.to_csv('temp.csv', index=False)

spark_df = spark.read.csv('temp.csv', header=True, inferSchema=True)

# Show the Spark DataFrame
spark_df.show(5)
```
````

````{tabs}

```{code-tab} plaintext Python Output
+-----------------+--------------------+--------+---------------+--------------------+-----------------+-----------+--------------+-----------+----+
|             Name|             Address|Postcode|           City|               Email|Mode_of_Transport|  Education|Marital_Status|Supermarket| Pet|
+-----------------+--------------------+--------+---------------+--------------------+-----------------+-----------+--------------+-----------+----+
| William Jennings|2 Sian streets, N...| L8G 7YL|Port Samchester| ricky23@example.com|              Car|       null|       Widowed|      Tesco| Cat|
|Dr Josh Pritchard|Studio 16, Lynn h...|  S4 5GQ|       Rhysview|johnronald@exampl...| Public Transport|High School|       Widowed|  Morrisons| Cat|
|    Leigh Randall|0 Alexander circl...|NW8M 9RQ|   East Natasha|  dgreen@example.org| Public Transport|        PhD|       Married|       null|None|
|  Lorraine Palmer|Studio 01, Read j...|MK9H 5GX|New Brandonfort|griffithslinda@ex...| Public Transport|    Primary|       Married|   Waitrose|null|
|       June Sharp|782 Hill rest, Ar...| W1D 1PA|     New Gerald|hammondjulia@exam...| Public Transport|    Primary|        Single|       Asda| Dog|
+-----------------+--------------------+--------+---------------+--------------------+-----------------+-----------+--------------+-----------+----+
only showing top 5 rows
```
````
### Conclusion
Generating synthetic data is a crucial tool for testing algorithms, saving resources, and maintaining privacy. Faker offers various providers to generate synthetic data on Python and big data frameworks like `PySpark`  can help handle large datasets efficiently.


### References and further reading
* [Faker Documentation (Python)](https://faker.readthedocs.io/en/master/)
* [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
* [Synthpop: Syntheic Data in `R`](../ancillary-topics/synthpop_with_r)
* [Mimesis: Synthetic Data in Python](../ancillary-topics/mimesis)