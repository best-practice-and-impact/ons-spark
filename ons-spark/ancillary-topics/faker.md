## Faker: Synthetic Data in Python

### 1. Introduction to Faker  
`Faker` is a Python package that generates fake data such as names, addresses, emails, dates, credit card numbers, and more. It is similar to [`Mimesis`](../ancillary-topics/mimesis.html), another Python package for synthetic data generation.

Faker is widely used for creating realistic test data for software development, data science, and machine learning projects. By generating synthetic data that mimics real-world information, you can test your applications, validate models, and protect sensitive information without exposing real data.

This guide will also show how to use Faker with `PySpark` for generating synthetic data at scale, making it suitable for both small and big data applications.

**How does Faker work?**

Faker uses a modular system of "providers," where each provider is responsible for generating a specific type of data (such as names, addresses, or dates). You can select different locales to generate data that matches the conventions of various countries, making it suitable for international applications. Faker also supports seeding, which allows you to generate reproducible datasets for debugging and testing.

**Key Features:** Randomised generation, locale support, wide range of data types, reproducibility, and extensibility.

### 2. Install and initialise Faker
To start, we will need the `Faker` library to generate the dummy data. Additionally, we will use PySpark for working with Spark DataFrames in Python.

This guide will also show how to use Faker with PySpark for generating synthetic data at scale, making it suitable for both small and big data applications.

#### 2.1 Install Faker
````{tabs}
```{code-tab} py
pip install Faker
```
````
For further details on the Faker package, please visit the [official documentation](https://faker.readthedocs.io/en/master/).

#### 2.2 Check the version of the Faker package installed in your environment
It is worth noting the version of the Faker package installed in your environment. This is important because different versions may have changes in functionality, seeding methods, or compatibility with other packages, which can affect reproducibility and integration with your code.

````{tabs}
```{code-tab} py
import pkg_resources
faker_version = pkg_resources.get_distribution("Faker").version
print(f"faker_version: {faker_version}")
```
````

```plaintext
faker_version: 37.3.0
```
#### 2.3 Simple example of using Faker

Faker is designed to generate realistic synthetic data by using a modular system of "providers." Each provider is responsible for producing a specific type of data, such as names, addresses, dates, or company information. This modular approach allows you to easily access a wide variety of fake data types and combine them as needed for your use case.

Faker supports multiple locales, enabling you to generate data that matches the conventions and formats of different countries and regions. This is especially useful for testing applications that require internationalisation or region-specific data.

To ensure reproducibility, Faker allows you to set a random seed. Setting this seed allows us to generate the exact same synthetic data as has been/will be shown in our examples. This is especially helpful when we are debugging or creating tests that require consistent data.

The following example demonstrates how to generate basic synthetic personal data using a seeded Faker instance.

Let’s walk through a simple example of generating basic synthetic data using a seeded Faker instance:
````{tabs}
```{code-tab} py
from faker import Faker

# Initialise Faker with a specific locale and seed for reproducibility
fake_uk = Faker('en_GB')
fake_uk.seed_instance(42)

print("Name:", fake_uk.name())
print("Email:", fake_uk.email())
print("Address:", fake_uk.address().replace("\n", ", ")) # the address method has new line characters (\n), hence, replace with ", " to have all in one line
print("Job:", fake_uk.job())
print("Company:", fake_uk.company())
print("Phone:", fake_uk.phone_number())
print("Birthday:", fake_uk.date_of_birth(minimum_age=18, maximum_age=90))
```
````

```plaintext
Name: William Jennings
Email: duncan32@example.net
Address: 600 Charlie fort, New Joeside, DT79 0GS
Job: Librarian, public
Company: Bryan-Andrews
Phone: 01314960116
Birthday: 1947-08-03
```
Each call to a provider method (such as `fake_uk.name()` or `fake_uk.address()`) returns a realistic value that matches the selected locale. By combining different providers, you can quickly build complex and realistic synthetic datasets for your projects.

### 3. Faker localisation

Faker has a great feature that allows it to generate data tailored to specific locales. To generate data for a specific country or region, simply pass the desired locale code (such as `'en_GB'` for the UK or `'fr_FR'` for France) as an argument when creating your Faker instance.

The example below demonstrates how address formats differ across locales using Faker.
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

```plaintext
US: 7171 Rhonda Squares Apt. 506, Wesleyton, LA 16408
UK: 368 Griffiths wall, Lake Michelle, CO4W 7GQ
France: 4, rue de Guillot, 48189 RivièreBourg
Japan: 東京都中央区一ツ橋12丁目16番18号
```
Notice the differences in address formats:

* US addresses include state abbreviations and ZIP codes.  
* UK addresses use British postal codes.  
* France addresses follow European conventions.  
* Japan addresses are formatted with the correct characters and local conventions.


For an up-to-date list of supported locales, you can check the [official documentation](https://fakerjs.dev/guide/localization.html#available-locales).

### 4. Faker’s provider architecture

Faker utilises a modular system of "providers," where each provider is responsible for generating a specific type of data. Let's take a closer look at how providers work and the types available by using fake data for the UK locale.


The following example lists all available providers for a given Faker instance.
````{tabs}
```{code-tab} py
print("Available Faker Providers:")
for provider in fake_uk.providers:
    print(f"- {provider}")
```
````

```plaintext
Available Faker Providers:
- <faker.providers.user_agent.Provider object at 0x000001D7D59674C0>
- <faker.providers.ssn.en_GB.Provider object at 0x000001D7D59675B0>
- <faker.providers.sbn.Provider object at 0x000001D7D5967550>
- <faker.providers.python.Provider object at 0x000001D7D5967400>
- <faker.providers.profile.Provider object at 0x000001D7D5967490>
- <faker.providers.phone_number.en_GB.Provider object at 0x000001D7D5967340>
- <faker.providers.person.en_GB.Provider object at 0x000001D7D59673D0>
- <faker.providers.passport.en_US.Provider object at 0x000001D7D5967100>
- <faker.providers.misc.en_US.Provider object at 0x000001D7D5967310>
- <faker.providers.lorem.la.Provider object at 0x000001D7D59672B0>
- <faker.providers.job.en_US.Provider object at 0x000001D7D5967250>
- <faker.providers.isbn.en_US.Provider object at 0x000001D7D59671F0>
- <faker.providers.internet.en_GB.Provider object at 0x000001D7D5967190>
- <faker.providers.geo.en_US.Provider object at 0x000001D7D5966FE0>
- <faker.providers.file.Provider object at 0x000001D7D59670D0>
- <faker.providers.emoji.Provider object at 0x000001D7D5967070>
- <faker.providers.doi.Provider object at 0x000001D7D5967010>
- <faker.providers.date_time.en_US.Provider object at 0x000001D7D5966FB0>
- <faker.providers.currency.en_US.Provider object at 0x000001D7D5966F50>
- <faker.providers.credit_card.en_US.Provider object at 0x000001D7D5966EF0>
- <faker.providers.company.en_US.Provider object at 0x000001D7D5966E90>
- <faker.providers.color.en_US.Provider object at 0x000001D7D5966E30>
- <faker.providers.barcode.en_US.Provider object at 0x000001D7D5966DD0>
- <faker.providers.bank.en_GB.Provider object at 0x000001D7D5966D70>
- <faker.providers.automotive.en_GB.Provider object at 0x000001D7D5966D10>
- <faker.providers.address.en_GB.Provider object at 0x000001D7D5966C80>
```
Each item in the list corresponds to a specific provider, such as:

- **User Agent Provider:** Generates browser and device identification strings.  
- **SSN Provider:** Creates valid-format social security numbers (US-specific).  
- **SBN Provider:** Generates 9-digit Standard Book Numbers (used in older systems prior to 1974).  

In addition to these, Faker includes other commonly used providers for generating various types of data:

- **Person Provider:** Generates names, birthdates, and personal details.  
- **Address Provider:** Produces realistic street addresses and postal codes.
- **Internet Provider:** Generates email addresses, domain names, and URLs.

You can explore the full list of available providers and their methods in the [official documentation](https://faker.readthedocs.io/en/master/providers.html), which offers detailed information and usage examples for each one.

### 5. Best practices for using Faker

When working with Faker, keep the following best practices in mind:

- Select locale-specific providers when generating data tailored to a particular region.
- Combine different providers to generate more realistic and interconnected data.
- Organise your fake data generation to align with the requirements of your application.
- Explore the official documentation to learn about additional provider features and options.

### 6. Building a typical dataset

Now that we are familiar with how to set up Faker and its key features, let's explore how to use it in practice. In the following example, we will build a typical fake dataset using data generated for the UK locale to create user profiles. This will demonstrate how to combine multiple providers to generate rich and interconnected data for more complex structures.


The example below shows how to generate a DataFrame of user profiles with multiple attributes using Faker.
````{tabs}
```{code-tab} py
from faker import Faker
import pandas as pd

fake_uk = Faker('en_GB')
fake_uk.seed_instance(42)

n_rows = 100


def generate_user_profile():
    """ Function to generate a user profile """
    return {
        'name': fake_uk.name(),
        'age': fake_uk.random_int(min=18, max=80),
        'email': fake_uk.email(),
        
        'street': fake_uk.street_address().replace("\n", " "),
        'city': fake_uk.city(),
        'postcode': fake_uk.postcode(),
        
        'job': fake_uk.job(),
        'company': fake_uk.company(),
        
        'username': fake_uk.user_name(),
        'website': fake_uk.url()
    }
 
profiles = [generate_user_profile() for _ in range(n_rows)]
 
df = pd.DataFrame(profiles)

df.head()
```
````

```plaintext
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
### 7. Using synthetic data with big data frameworks

Generating synthetic data at scale is essential for testing big data pipelines, validating distributed systems, and simulating real-world scenarios. In this section, we provide two worked examples of using Faker with PySpark: the first demonstrates a simple integration, while the second adds additional features to reflect more realistic and complex datasets.

#### 7.1. Using synthetic data with PySpark

Demonstrate how to create synthetic data in PySpark and use it within Spark DataFrames.


The following example shows how to generate a simple synthetic dataset and load it into a Spark DataFrame.
````{tabs}
```{code-tab} py
# !pip install pyspark
from pyspark.sql import SparkSession
from faker import Faker

spark = SparkSession.builder.master("local[2]").appName("Synthetic Data Example").getOrCreate()

fake_uk = Faker('en_GB')
fake_uk.seed_instance(42)

n_rows = 1000

data = [
    (
        fake_uk.name(),
        fake_uk.address().replace("\n", ", "), 
        fake_uk.email()
    ) 
    for _ in range(n_rows)
    ]

columns = ["Name", "Address", "Email"]

df_spark = spark.createDataFrame(data, schema=columns)

df_spark.show(5)
spark.stop()
```
````
**Application to Big Data:**

This example can be extended to generate large datasets (millions of records) that can be processed in parallel using PySpark.

#### 7.2. Generate synthetic data with more features

While Faker provides a wide range of data types, it may not cover every feature you need for your use case. In this example, we combine Faker-generated data with custom-created features (such as mode of transport, education level, etc.) to build a more comprehensive and realistic dataset.


This example demonstrates how to generate a more complex synthetic dataset with additional categorical features and missing values, and load it directly into a Spark DataFrame.

We will generate synthetic data with the following additional features:
1. Mode of Transportation (Car, Public Transport, Walking, etc.)  
2. Highest Education (High School, Bachelor's, Master's, PhD, etc.)  
3. Marital Status (Single, Married, Divorced, Widowed)  
4. Favorite High Street Supermarket (Tesco, Sainsbury's, Asda, etc.)  
5. Pet Ownership (Yes, No - with type of pet if Yes)  

We will also simulate some missing data in the dataset, which is commonly encountered in real-world scenarios.
````{tabs}
```{code-tab} py
from faker import Faker
from faker.generator import random
from pyspark.sql import SparkSession

# Initialise Spark session
spark = SparkSession.builder.master("local[2]").appName("SyntheticDataForBigData").getOrCreate()


fake_uk = Faker('en_GB')
fake_uk.seed_instance(42)

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
    name = fake_uk.name()
    address = fake_uk.address()
    postcode = fake_uk.postcode()
    city = fake_uk.city()
    email = fake_uk.email()
    transport = fake_uk.random_element(mode_of_transport)
    education = fake_uk.random_element(education_levels)
    marital = fake_uk.random_element(marital_status)
    supermarket = fake_uk.random_element(supermarkets)
    pet = fake_uk.random_element(pets)
    
    # Simulating missing data by randomly omitting some features
    if random.random() < 0.1:
        transport = None
    if random.random() < 0.1:
        education = None
    if random.random() < 0.1:
        marital = None
    if random.random() < 0.1:
        supermarket = None
    if random.random() < 0.1:
        pet = None
        
    data.append([name, address, postcode, city, email, transport, education, marital, supermarket, pet])


columns = ['Name', 'Address', 'Postcode', 'City', 'Email', 'Mode_of_Transport', 'Education', 'Marital_Status', 'Supermarket', 'Pet']

# Create Spark DataFrame directly from the list of tuples
synthetic_spark_df = spark.createDataFrame(data, schema=columns)

synthetic_spark_df.show(5)

# Close the Spark session to free up resources
spark.stop()
```
````

```plaintext
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
### Conclusion
Generating synthetic data is a crucial tool for testing algorithms, saving resources, and maintaining privacy. Faker offers various providers to generate synthetic data on Python and big data frameworks like `PySpark`  can help handle large datasets efficiently.


### References and further reading
* [Faker Documentation (Python)](https://faker.readthedocs.io/en/master/)
* [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
* [Synthpop: Synthetic Data in R](../ancillary-topics/synthpop_with_r)
* [Mimesis: Synthetic Data in Python](../ancillary-topics/mimesis)