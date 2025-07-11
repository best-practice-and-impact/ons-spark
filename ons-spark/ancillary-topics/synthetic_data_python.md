## Synthetic Data Generation in Python: Faker and Mimesis

### 1. Introduction

This is a guide on generating synthetic data in Python. Synthetic data is artificially created information that closely resembles real-world data, but contains no actual personal or sensitive information. It is invaluable for:

- Testing and validating data pipelines and machine learning models
- Protecting privacy and confidentiality
- Simulating rare or edge-case scenarios
- Enabling reproducible research and open data sharing

In this notebook, we demonstrate how to generate synthetic data using two leading Python libraries: [Faker](https://faker.readthedocs.io/en/master/) and [Mimesis](https://mimesis.name/). For each example, we show parallel code for both libraries, highlighting their similarities and differences. We also cover best practices, advanced use cases, and how to scale up data generation with PySpark for big data applications.

By the end of this notebook, you will be able to:
- Generate realistic synthetic data for a variety of domains
- Choose between Faker and Mimesis based on your needs
- Understand how to ensure reproducibility and locale-specific data
- Scale up data generation for large datasets
- Apply best practices for synthetic data projects

### 2. Install and setup

First, install the required libraries and check their versions to ensure compatibility.
````{tabs}
```{code-tab} py Faker
!pip install faker
```

```{code-tab} py Mimesis
!pip install mimesis
```
````

````{tabs}
```{code-tab} py Faker
import faker
import pkg_resources
print("Faker version:", pkg_resources.get_distribution("Faker").version) 
```

```{code-tab} py Mimesis
import mimesis
print("Mimesis version:", mimesis.__version__)
```
````

````{tabs}
```{code-tab} plaintext Faker Output
Faker version: 37.4.0
```

```{code-tab} plaintext Mimesis Output
Mimesis version: 18.0.0
```
````

### 3. Setting a random seed

Setting a random seed ensures reproducibility, so you get the same synthetic data each time you run the code.

````{tabs}
```{code-tab} py Faker
from faker import Faker

# Set up Faker
fake_uk = Faker('en_GB')
fake_uk.seed_instance(42)
```
```{code-tab} py Mimesis
from mimesis import Generic
from mimesis import random as mimesis_random

# Set up Mimesis
mimesis_random.global_seed = 42
generic = Generic(locale='en_GB', seed=42)
```
````

### 4. Providers and Locales in Faker and Mimesis

Both Faker and Mimesis use the concept of **providers** (modules or classes that generate specific types of data) and **locales** (to match country or language conventions). Below, we show how to list available providers and generate data for different locales.

#### 4.1 Providers

You can explore the full list of available providers and their methods in the official [Faker](https://faker.readthedocs.io/en/master/providers.html) and [Mimesis](https://mimesis.name/v12.1.1/providers.html) documentations, which offer detailed information and usage examples for each one.

````{tabs}
```{code-tab} py Faker
print("Faker Providers:")
for provider in fake_uk.providers:
    print("-", provider)
```
```{code-tab} py Mimesis
print("Mimesis Providers:")
for attribute in dir(generic):
    if not attribute.startswith('_'):
        print("-", attribute)
```
````

````{tabs}
```{code-tab} plaintext Faker Output
Faker Providers:
- <faker.providers.user_agent.Provider object at 0x00000199DED9E740>
- <faker.providers.ssn.en_GB.Provider object at 0x00000199DED9F670>
- <faker.providers.sbn.Provider object at 0x00000199DED9C610>
- <faker.providers.python.Provider object at 0x00000199DED9C1F0>
- <faker.providers.profile.Provider object at 0x00000199DED9E470>
- <faker.providers.phone_number.en_GB.Provider object at 0x00000199DED9FD60>
- <faker.providers.person.en_GB.Provider object at 0x00000199DED9C070>
- <faker.providers.passport.en_US.Provider object at 0x00000199DED9CAC0>
- <faker.providers.misc.en_US.Provider object at 0x00000199DED9FDF0>
- <faker.providers.lorem.la.Provider object at 0x00000199DED9FE80>
- <faker.providers.job.en_US.Provider object at 0x00000199DED9C820>
- <faker.providers.isbn.en_US.Provider object at 0x00000199DED9C880>
- <faker.providers.internet.en_GB.Provider object at 0x00000199DED9C9A0>
- <faker.providers.geo.en_US.Provider object at 0x00000199DED9C8B0>
- <faker.providers.file.Provider object at 0x00000199DED9F7F0>
- <faker.providers.emoji.Provider object at 0x00000199DED9F8B0>
- <faker.providers.doi.Provider object at 0x00000199DED9F910>
- <faker.providers.date_time.en_US.Provider object at 0x00000199DED9FA00>
- <faker.providers.currency.en_US.Provider object at 0x00000199DED9DB10>
- <faker.providers.credit_card.en_US.Provider object at 0x00000199DED9E5C0>
- <faker.providers.company.en_US.Provider object at 0x00000199DED9E6B0>
- <faker.providers.color.en_US.Provider object at 0x00000199DED9DCC0>
- <faker.providers.barcode.en_US.Provider object at 0x00000199DED9EB00>
- <faker.providers.bank.en_GB.Provider object at 0x00000199DED9DBD0>
- <faker.providers.automotive.en_GB.Provider object at 0x00000199DEDC9AE0>
- <faker.providers.address.en_GB.Provider object at 0x00000199DD272AD0>
```
```{code-tab} plaintext Mimesis Output
Mimesis Providers:
- address
- binaryfile
- choice
- code
- cryptographic
- datetime
- development
- file
- finance
- food
- hardware
- internet
- numeric
- path
- payment
- person
- science
- text
- transport
```
````

#### 4.2 Locales

Locales allow you to generate data that matches the conventions and formats of different countries and regions. This is useful for internationalisation and region-specific testing.


To demonstrate how locales influence data generation, let us create a simple example that generates person and address data for various regions.  Note that both libraries also support language-specific data generation; for example, outputs for Arabic and Japanese locales are written in the respective languages. For a full list of supported locales, please refer to the official [Faker](https://fakerjs.dev/guide/localization.html#available-locales) and [Mimesis](https://mimesis.name/v12.1.1/locales.html) documentations.


**Generate data for multiple locales**

````{tabs}
```{code-tab} py Faker
print("\nFaker Examples for Multiple Locales:")
locales = ['en_GB', 'en_US', 'ja_JP', 'fr_FR', 'ar']
for loc in locales:
    fake = Faker(loc)
    fake.seed_instance(42)
    print(f"\n{loc.upper()} Example:")
    print("Name:", fake.name())
    print("Email:", fake.email())
    print("Job:", fake.job())
```

```{code-tab} py Mimesis
from mimesis import Person
print("\nMimesis Examples for Multiple Locales:")
locales = [Locale.EN_GB, Locale.EN, Locale.JA, Locale.FR, Locale.AR_EG]
for loc in locales:
    person = Person(locale=loc, seed=42)
    print(f"\n{loc.value.upper()} Example:")
    print("Full Name:", person.full_name())
    print("Email:", person.email())
    print("Job:", person.occupation())
```
````

````{tabs}
```{code-tab} plaintext Faker Output
Faker Examples for Multiple Locales:

EN_GB Example:
Name: William Jennings
Email: duncan32@example.net
Job: Civil engineer, consulting

EN_US Example:
Name: Allison Hill
Email: donaldgarcia@example.net
Job: Sports administrator

JA_JP Example:
Name: 佐藤 淳
Email: tomoyatakahashi@example.org
Job: 測量士

FR_FR Example:
Name: Alexandre Traore
Email: patrick32@example.net
Job: boucher

AR Example:
Name: جلاء بنو العريج
Email: dyaalhaby@example.net
Job: رجل مباحث
```
```{code-tab} plaintext Mimesis Output
Mimesis Examples for Multiple Locales:
EN-GB Example:
Full Name: Anthony Reilly
Email: holds1871@live.com
Job: Choreographer

EN Example:
Full Name: Anthony Reilly
Email: holds1871@live.com
Job: Choreographer

JA Example:
Full Name: 石松 田場
Email: gilbert1852@yahoo.com
Job: 巫女

FR Example:
Full Name: Alexy Rigal
Email: holds1871@live.com
Job: Responsable SAV (Service après-vente)

AR-EG Example:
Full Name: أيمن عبد الماجد باشا
Email: gilbert1852@yahoo.com
Job: تقني تدفئة وتكييف
```
````

#### 4.3 Generate user profiles with multiple providers

Combining multiple providers allows you to create rich, realistic user profiles for testing and simulation.

````{tabs}
```{code-tab} py Faker
profile = {
    "Name": fake_uk.name(),
    "Address": fake_uk.address().replace("\n", ", "),
    "Email": fake_uk.email(),
    "Job": fake_uk.job(),
    "Phone": fake_uk.phone_number(),
    "Company": fake_uk.company(),
    "Date of Birth": fake_uk.date_of_birth(minimum_age=18, maximum_age=90)
}
print("Faker User Profile:")
for k, v in profile.items():
    print(f"{k}: {v}")
```

```{code-tab} py Mimesis
from mimesis import Address, Finance, Datetime

person = Person(locale=Locale.EN_GB, seed=42)
address = Address(locale=Locale.EN_GB, seed=42)
finance = Finance(locale=Locale.EN_GB, seed=42)
datetime = Datetime(locale=Locale.EN_GB, seed=42)
profile_mimesis = {
    "Name": person.full_name(),
    "Address": address.address(),
    "Email": person.email(),
    "Job": person.occupation(),
    "Phone": person.telephone(),
    "Company": finance.company(),
    "Date of Birth": datetime.formatted_date(fmt='%Y-%m-%d', start=1970, end=2005)
}
print("Mimesis User Profile:")
for k, v in profile_mimesis.items():
    print(f"{k}: {v}")
```
````

````{tabs}
```{code-tab} plaintext Faker Output
Faker User Profile:
Name: William Jennings
Address: 2 Sian streets, New Maryton, E3 8ZA
Email: francescaharrison@example.com
Job: English as a second language teacher
Phone: +44151 496 0940
Company: Harrison LLC
Date of Birth: 1972-04-21
```
```{code-tab} plaintext Mimesis Output
Mimesis User Profile:
Name: Anthony Reilly
Address: 1310 Blaney Avenue
Email: holds1871@live.com
Job: Choreographer
Phone: 01002 65774
Company: Centrica
Date of Birth: 1977-01-24
```
````

#### 4.4 Generating text using Faker and Mimesis

Generating synthetic text is useful for testing NLP pipelines, populating free-text fields, or simulating survey responses. Here, we show how to generate random sentences, words, paragraphs, and quotes using both Faker and Mimesis, side by side.

````{tabs}
```{code-tab} py Faker
fake = Faker()

print('Sentence:', fake.sentence())
print('Paragraph:', fake.paragraph())
print('Text:', fake.text(max_nb_chars=100))
print('Word:', fake.word())
print('Quote:', fake.catch_phrase())
```
```{code-tab} py Mimesis
from mimesis import Text

text = Text()

print('Sentence:', text.sentence())
print('Paragraph:', text.text(quantity=1))
print('Text:', text.text(quantity=2))
print('Word:', text.word())
print('Quote:', text.quote())
```
````

````{tabs}
```{code-tab} plaintext Faker Output
Sentence: Other hotel financial want nation.
Paragraph: Kitchen together begin town draw story capital piece. Full which agency democratic election cold although because.
Text: I president whatever very necessary seat color. Official behind yeah claim focus.
Word: team
Quote: Compatible modular adapter
```
```{code-tab} plaintext Mimesis Output
Sentence: Messages can be sent to and received from ports, but these messages must obey the so-called "port protocol."
Paragraph: Haskell is a standardized, general-purpose purely functional programming language, with non-strict semantics and strong static typing.
Text: Erlang is known for its designs that are well suited for systems. The sequential subset of Erlang supports eager evaluation, single assignment, and dynamic typing.
Word: recipes
Quote: I'm gonna make him an offer he can't refuse.
```
````

### 5. Generating data at scale with PySpark

For large datasets, you can use Faker or Mimesis to generate data and load it into a Spark DataFrame. Below are parallel examples for both libraries.

````{tabs}
```{code-tab} py Faker

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.master("local[2]").appName("SyntheticDataExample").getOrCreate()

n_rows = 1000


fake_uk = Faker('en_GB')
fake_uk.seed_instance(42)

faker_data = [(fake_uk.name(), fake_uk.address().replace("\n", ", "), fake_uk.email()) for _ in range(n_rows)] 
columns = ["Name", "Address", "Email"] 

# Check if in virtual environment
is_in_venv = os.getenv('VIRTUAL_ENV') is not None

if not is_in_venv:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: spark.createDataFrame")
    df_spark = spark.createDataFrame(faker_data, schema=columns)
else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame")
    import pandas as pd
    df_pd = pd.DataFrame(faker_data, columns=columns)

    file_path = "personal_data_temp.csv"
    df_pd.to_csv(file_path, index=False)
    df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

df_spark.show(5)
spark.stop()
```
```{code-tab} py Mimesis
import pandas as pd

spark = SparkSession.builder.master("local[2]").appName("MimesisDataGeneration").getOrCreate()

# Check if in virtual environment
is_in_venv = os.getenv('VIRTUAL_ENV') is not None

person = Person(locale=Locale.EN_GB, seed=42)
finance = Finance(locale=Locale.EN_GB, seed=42)

n_rows = 1000

mimesis_data = {
    "First Name": [person.first_name() for _ in range(n_rows)],
    "Last Name": [person.last_name() for _ in range(n_rows)],
    "Full Name": [person.full_name() for _ in range(n_rows)],
    "Gender": [person.gender() for _ in range(n_rows)],
    "Age": [person.random.randint(16, 88) for _ in range(n_rows)],
    "Email": [person.email() for _ in range(n_rows)],
    "Phone Number": [person.phone_number() for _ in range(n_rows)],
    "Nationality": [person.nationality() for _ in range(n_rows)],
    "Occupation": [person.occupation() for _ in range(n_rows)],
    "Bank Name": [finance.bank() for _ in range(n_rows)],
    "Company Name": [finance.company() for _ in range(n_rows)],
    "Company Type": [finance.company_type() for _ in range(n_rows)],
    "Cryptocurrency ISO Code": [finance.cryptocurrency_iso_code() for _ in range(n_rows)],
    "Cryptocurrency Symbol": [finance.cryptocurrency_symbol() for _ in range(n_rows)],
    "Currency ISO Code": [finance.currency_iso_code() for _ in range(n_rows)],
    "Currency Symbol": [finance.currency_symbol() for _ in range(n_rows)],
    "Random Price": [finance.price() for _ in range(n_rows)],
    "Price in BTC": [finance.price_in_btc() for _ in range(n_rows)],
    "Stock Exchange Name": [finance.stock_exchange() for _ in range(n_rows)],
    "Stock Name": [finance.stock_name() for _ in range(n_rows)],
    "Stock Ticker": [finance.stock_ticker() for _ in range(n_rows)]
}

if not is_in_venv:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: spark.createDataFrame")
    df_mimesis_spark = spark.createDataFrame(
        [(v) for v in zip(*mimesis_data.values())],
        schema=list(mimesis_data.keys())  
    )
else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame")
    df_mimesis_pd = pd.DataFrame(mimesis_data)

    file_path = "mimesis_data_temp.csv"
    df_mimesis_pd.to_csv(file_path, index=False)
    df_mimesis_spark = spark.read.csv(file_path, header=True, inferSchema=True)
     
df_mimesis_spark.show(5)
spark.stop()
```
````

````{tabs}
```{code-tab} plaintext Faker Output
is_in_venv: False | Spark DataFrame to be created from: spark.createDataFrame
                                                                                
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
```{code-tab} plaintext Mimesis Output
is_in_venv: False | Spark DataFrame to be created from: spark.createDataFrame
                                                                                
+----------------+------+---+--------------------+-------------+-----------+--------------+--------------------+
|       Full Name|Gender|Age|               Email| Phone Number|Nationality|    Occupation|           Bank Name|
+----------------+------+---+--------------------+-------------+-----------+--------------+--------------------+
|  Anthony Reilly|  Male| 45| saw1917@outlook.com|023 5108 9063|  Ethiopian|        Brewer|National Counties...|
|       Kaley Day| Other| 86|plan1855@protonma...|0893 286 9998|    Belgian| Horse Trainer|The Royal Bank of...|
|Cleveland Osborn| Other| 23|finland2058@duck.com|0111 971 3359| Guatemalan|        Driver|Royal Bank of Sco...|
|     Zack Holder|Female| 65|differ1890@proton...| 017582 12621| Ecuadorian|        Tailor|Paragon Banking G...|
|     Arden Brady| Other| 82| aud2057@example.com|  0800 204605|      Greek|Bakery Manager|     Triodos Bank UK|
+----------------+------+---+--------------------+-------------+-----------+--------------+--------------------+
only showing top 5 rows
```
````

### 6. Generating fake population data

Generating synthetic population data is essential for simulating census datasets, survey microdata, or anonymised samples. In this section, we demonstrate how to create tabular, population-like data with custom categories, such as sex, ethnicity, marital status, and employment status, while also simulating missing values, using the Mimesis library. The same approach and logic can be applied with the Faker library.

Note: To generate a name consistent with a specific gender, use the `.person().first_name()` and `.person().last_name()` methods separately, specifying the gender for the first name with the `Gender.MALE` or `Gender.FEMALE` enum from Mimesis. This ensures that gender-related fields (such as first name and title) are logically consistent within each synthetic record.

````{tabs}
```{code-tab} py Faker
from faker import Faker
import numpy as np
import random
import datetime
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

fake = Faker('en_GB')
Faker.seed(42)
random.seed(42)
np.random.seed(42)

n_rows = 1000
shared_address_rows = 10

schema = StructType([
    StructField('ID', StringType(), True),
    StructField('SOURCE_FILE', StringType(), True),
    StructField('TIME_STAMP', StringType(), True),
    StructField('Local_Authority', StringType(), True),
    StructField('Postcode', StringType(), True),
    StructField('Address_Line_1', StringType(), True),
    StructField('Address_Line_2', StringType(), True),
    StructField('Address_Line_3', StringType(), True),
    StructField('Address_Line_4', StringType(), True),
    StructField('Address_Line_5', StringType(), True),
    StructField('Address_Line_6', StringType(), True),
    StructField('Last_Name', StringType(), True),
    StructField('First_Name', StringType(), True),
    StructField('Sex', StringType(), True),
    StructField('Title', StringType(), True),
    StructField('Marital_Status', StringType(), True),
    StructField('DOB', StringType(), True),
    StructField('Ethnicity', StringType(), True),
    StructField('Education_Level', StringType(), True),
    StructField('Nationality', StringType(), True),
    StructField('Income', IntegerType(), True),
    StructField('Employment_Status', StringType(), True),
    StructField('Academic_Degree', StringType(), True),
    StructField('guid', StringType(), True)
])

categories = {
    'sex': ['Male', 'Female'],
    'marital_status': {
        'Male': ['Single', 'Married', 'Divorced', 'Widowed'],
        'Female': ['Single', 'Married', 'Divorced', 'Widowed']
    },
    'education_level': [
        'No Qualifications', 'GCSEs', 'A-Levels', 'Apprenticeship',
        "Bachelor's Degree", "Master's Degree", 'PhD'
    ],
    'ethnicity': ['White', 'Black', 'Asian', 'Mixed', 'Other'],
    'employment_status': ['Employed', 'Unemployed', 'Student', 'Retired'],
    'academic_degree': [
        "None", "Bachelor's Degree", "Master's Degree", "PhD", "Diploma", "Certificate"
    ],
    'nationality': [
        "British", "Irish", "Polish", "Indian", "Pakistani", "Nigerian", "Chinese", "Other"
    ]
}

def maybe_missing(val, p=0.1):
    return val if random.random() > p else None

def random_timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

shared_address = {
    'Local_Authority': fake.county(),
    'Postcode': fake.postcode(),
    'Address_Line_1': str(random.randint(1, 200)),
    'Address_Line_2': fake.street_name(),
    'Address_Line_3': None,
    'Address_Line_4': fake.city(),
    'Address_Line_5': fake.country(),
    'Address_Line_6': 'United Kingdom',
}

columns = [
    'ID', 'SOURCE_FILE', 'TIME_STAMP', 'Local_Authority', 'Postcode', 'Address_Line_1',
    'Address_Line_2', 'Address_Line_3', 'Address_Line_4', 'Address_Line_5', 'Address_Line_6',
    'Last_Name', 'First_Name', 'Sex', 'Title', 'Marital_Status', 'DOB', 'Ethnicity', 'Education_Level',
    'Nationality', 'Income', 'Employment_Status', 'Academic_Degree', 'guid'
]

rows = []
for i in range(n_rows):
    sex = random.choice(categories['sex'])
    first_name = fake.first_name_male() if sex == 'Male' else fake.first_name_female()
    title = random.choice(['Mr', 'Dr', 'Prof']) if sex == 'Male' else random.choice(['Ms', 'Mrs', 'Miss', 'Dr', 'Prof'])
    last_name = fake.last_name()
    marital_status = random.choice(categories['marital_status'][sex])
    if i >= n_rows - shared_address_rows:
        address = shared_address
    else:
        address = {
            'Local_Authority': fake.county(),
            'Postcode': fake.postcode(),
            'Address_Line_1': str(random.randint(1, 200)),
            'Address_Line_2': fake.street_name(),
            'Address_Line_3': None,
            'Address_Line_4': fake.city(),
            'Address_Line_5': fake.country(),
            'Address_Line_6': 'United Kingdom',
        }
    age = random.randint(18, 99)
    dob = (datetime.date.today() - datetime.timedelta(days=age*365 + random.randint(0, 364)))
    income_val = maybe_missing(random.randint(25000, 75000))
    income_val = int(income_val) if income_val is not None else None
    row = {
        'ID': str(i+1).zfill(len(str(n_rows))),
        'SOURCE_FILE': f"{fake.user_name()}.csv",
        'TIME_STAMP': random_timestamp(),
        **address,
        'Last_Name': last_name,
        'First_Name': first_name,
        'Sex': sex,
        'Title': title,
        'Marital_Status': marital_status,
        'DOB': dob.strftime('%d-%m-%Y'),
        'Ethnicity': maybe_missing(random.choice(categories['ethnicity'])),
        'Education_Level': maybe_missing(random.choice(categories['education_level'])),
        'Nationality': random.choice(categories['nationality']),
        'Income': income_val,
        'Employment_Status': maybe_missing(random.choice(categories['employment_status'])),
        'Academic_Degree': maybe_missing(random.choice(categories['academic_degree'])),
        'guid': fake.uuid4()
    }
    # Ensure all keys are present
    for col in columns:
        if col not in row:
            row[col] = None
    rows.append(row)

spark = SparkSession.builder.appName("FakerData").getOrCreate()
is_in_venv = os.getenv('VIRTUAL_ENV') is not None

if is_in_venv:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: CSV file")
    file_path = "population_data_temp_faker.csv"
    import csv
    with open(file_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    population_df = spark.read.csv(file_path, header=True, schema=schema)
else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: spark.createDataFrame")
    population_df = spark.createDataFrame(rows, schema=schema)

# To display the first 9 columns and 5 rows
population_df.select(population_df.columns[:9]).show(5)

# Or if you want to show specific columns
cols_to_show = ["ID", "First_Name", "Last_Name", "Title", "Sex", "DOB", "Ethnicity", "Address_Line_1", "Address_Line_2", "Postcode"]
population_df.select(cols_to_show).show(5)
spark.stop()
```
```{code-tab} py Mimesis
from mimesis import Generic
from mimesis.locales import Locale
from mimesis.enums import Gender, TitleType
import pandas as pd
import numpy as np
import random
import datetime

generic = Generic(locale=Locale.EN_GB, seed=42)

n_rows = 1000
shared_address_rows = 10

categories = {
    'sex': ['Male', 'Female'],
    'marital_status': {
        'Male': ['Single', 'Married', 'Divorced', 'Widowed'],
        'Female': ['Single', 'Married', 'Divorced', 'Widowed']
    },
    'education_level': [
        'No Qualifications',
        'GCSEs',
        'A-Levels',
        'Apprenticeship',
        "Bachelor's Degree",
        "Master's Degree",
        'PhD'
    ],
    'ethnicity': ['White', 'Black', 'Asian', 'Mixed', 'Other'],
    'employment_status': ['Employed', 'Unemployed', 'Student', 'Retired']
}

def maybe_missing(val, p=0.1):
    return val if random.random() > p else None

def random_timestamp():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Shared address for a group
shared_address = {
    'Local_Authority': generic.address.region(),
    'Postcode': generic.address.postal_code(),
    'Address_Line_1': str(generic.person.random.randint(1, 200)),
    'Address_Line_2': generic.address.street_name(),
    'Address_Line_3': np.nan,
    'Address_Line_4': generic.address.city(),
    'Address_Line_5': generic.address.country(),
    'Address_Line_6': 'United Kingdom',
}

data = {
    'Local_Authority': [],
    'Postcode': [],
    'Address_Line_1': [],
    'Address_Line_2': [],
    'Address_Line_3': [],
    'Address_Line_4': [],
    'Address_Line_5': [],
    'Address_Line_6': [],
    'Last_Name': [],
    'First_Name': [],
    'Sex': [],
    'Title': [],
    'Marital_Status': [],
    'guid': [],
    'SOURCE_FILE': [],
    'DOB': [],
    'Ethnicity': [],
    'Education_Level': [],
    'TIME_STAMP': [],
    'Nationality': [],
    'Income': [],
    'Employment_Status': [],
    'Academic_Degree': []
}

for i in range(n_rows):
    # Assign sex
    sex = random.choice(categories['sex'])
    gender_enum = Gender.MALE if sex == 'Male' else Gender.FEMALE
    # First name and title consistent with sex
    first_name = generic.person.first_name(gender=gender_enum)
    title = generic.person.title(gender=gender_enum, title_type=TitleType.TYPICAL)
    # Marital status consistent with sex
    marital_status = random.choice(categories['marital_status'][sex])
    # Shared address for last shared_address_rows
    if i >= n_rows - shared_address_rows:
        for k in shared_address:
            data[k].append(shared_address[k])
    else:
        data['Local_Authority'].append(generic.address.region())
        data['Postcode'].append(generic.address.postal_code())
        data['Address_Line_1'].append(str(generic.person.random.randint(1, 200)))
        data['Address_Line_2'].append(generic.address.street_name())
        data['Address_Line_3'].append(np.nan)
        data['Address_Line_4'].append(generic.address.city())
        data['Address_Line_5'].append(generic.address.country())
        data['Address_Line_6'].append('United Kingdom')
    # Names and other fields
    last_name = generic.person.last_name()
    data['Last_Name'].append(last_name)
    data['First_Name'].append(first_name)
    data['Sex'].append(sex)
    data['Title'].append(title)
    data['Marital_Status'].append(marital_status)
    data['guid'].append(generic.person.password())
    data['SOURCE_FILE'].append(f"{generic.person.username(mask='l_l')}.csv")
    # Date of Birth: Randomly generate between ages 18-99
    dob_year = generic.person.random.randint(1925, 2007)
    dob_month = generic.person.random.randint(1, 12)
    dob_day = generic.person.random.randint(1, 28)
    data['DOB'].append(f"{dob_day:02d}-{dob_month:02d}-{dob_year}")
    data['Ethnicity'].append(maybe_missing(random.choice(categories['ethnicity'])))
    data['Education_Level'].append(maybe_missing(random.choice(categories['education_level'])))
    data['TIME_STAMP'].append(random_timestamp())
    data['Nationality'].append(generic.person.nationality())
    data['Income'].append(maybe_missing(generic.random.randint(25000, 75000)))
    data['Employment_Status'].append(maybe_missing(generic.random.choice(categories['employment_status'])))
    data['Academic_Degree'].append(maybe_missing(generic.person.academic_degree()))

df = pd.DataFrame(data)
# Add ID column starting from 1
df['ID'] = [str(i).zfill(len(str(n_rows))) for i in np.arange(1, n_rows + 1)]
# Reorder columns
df = df[[
    'ID', 'SOURCE_FILE', 'TIME_STAMP', 'Local_Authority', 'Postcode', 'Address_Line_1',
    'Address_Line_2', 'Address_Line_3', 'Address_Line_4', 'Address_Line_5', 'Address_Line_6',
    'Last_Name', 'First_Name', 'Sex', 'Title', 'Marital_Status', 'DOB', 'Ethnicity', 'Education_Level',
    'Nationality', 'Income', 'Employment_Status', 'Academic_Degree', 'guid'
]]
df.head(12)

spark = SparkSession.builder.appName("MimesisData").getOrCreate()

# Check if in virtual environment
is_in_venv = os.getenv('VIRTUAL_ENV') is not None

if is_in_venv:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame")
    file_path = "population_data_temp.csv"
    df.to_csv(file_path, index=False)
    population_df = spark.read.csv(file_path, header=True, inferSchema=True)

else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: spark.createDataFrame")
    pd.DataFrame.iteritems = pd.DataFrame.items
    population_df = spark.createDataFrame(df)
    
# To display the first 9 columns and 5 rows
population_df.select(population_df.columns[:9]).show(5)

# Or if you want to show specific columns
cols_to_show = ["ID", "First_Name", "Last_Name", "Title", "Sex", "DOB", "Ethnicity", "Address_Line_1", "Address_Line_2", "Postcode"]
population_df.select(cols_to_show).show(5)
spark.stop()
```
````

````{tabs}
```{code-tab} plaintext Faker Output
is_in_venv: False | Spark DataFrame to be created from: spark.createDataFrame
                                                                                
+----+----------------+-------------------+---------------+--------+--------------+---------------+--------------+-----------------+
|  ID|     SOURCE_FILE|         TIME_STAMP|Local_Authority|Postcode|Address_Line_1| Address_Line_2|Address_Line_3|   Address_Line_4|
+----+----------------+-------------------+---------------+--------+--------------+---------------+--------------+-----------------+
|0001|  evansleigh.csv|2025-07-01 14:05:16|   Glasgow City| S37 0GS|            63|    White forge|          null|      Timothyland|
|0002|palmersandra.csv|2025-07-01 14:05:16|  Pembrokeshire|  M9 9HD|            57|    Irene forks|          null|        Jasonbury|
|0003|  wilsonryan.csv|2025-07-01 14:05:16|       Stirling| SN1 8JG|            89|     Lamb route|          null|      Lake Victor|
|0004|      ievans.csv|2025-07-01 14:05:16|       Highland|HD3X 6TF|            60|Davidson summit|          null|     Fionachester|
|0005|     jemma51.csv|2025-07-01 14:05:16|     Lancashire|CW6W 6RZ|            98|     Hill plaza|          null|Lake Geoffreyside|
+----+----------------+-------------------+---------------+--------+--------------+---------------+--------------+-----------------+
only showing top 5 rows

+----+----------+---------+-----+------+----------+---------+--------------+---------------+--------+
|  ID|First_Name|Last_Name|Title|   Sex|       DOB|Ethnicity|Address_Line_1| Address_Line_2|Postcode|
+----+----------+---------+-----+------+----------+---------+--------------+---------------+--------+
|0001|    Howard|   Foster|   Mr|  Male|03-05-1979|     null|            63|    White forge| S37 0GS|
|0002|   Bernard|Wilkinson| Prof|  Male|22-09-1949|    White|            57|    Irene forks|  M9 9HD|
|0003|    Sheila|    Evans|   Ms|Female|12-03-1930|    Other|            89|     Lamb route| SN1 8JG|
|0004|      Ryan|    Kelly|   Dr|  Male|27-12-1994|    Asian|            60|Davidson summit|HD3X 6TF|
|0005|     Peter|   Taylor|   Mr|  Male|21-08-1972|    Asian|            98|     Hill plaza|CW6W 6RZ|
+----+----------+---------+-----+------+----------+---------+--------------+---------------+--------+
only showing top 5 rows

```
```{code-tab} plaintext Mimesis Output
is_in_venv: False | Spark DataFrame to be created from: spark.createDataFrame
                                                                                
+----+--------------------+-------------------+----------------+--------+--------------+--------------+--------------+--------------+
|  ID|         SOURCE_FILE|         TIME_STAMP| Local_Authority|Postcode|Address_Line_1|Address_Line_2|Address_Line_3|Address_Line_4|
+----+--------------------+-------------------+----------------+--------+--------------+--------------+--------------+--------------+
|0001|guitars_appeared.csv|2025-07-01 13:57:58|   County Armagh|FN0F 6OF|           190|   Invergourie|           NaN|         Luton|
|0002|motors_chronicle.csv|2025-07-01 13:57:58|            Avon|TE4H 2TC|            57|  Druminiskill|           NaN|     Charlbury|
|0003|  buffalo_grande.csv|2025-07-01 13:57:58|         Norfolk|PU7N 9JO|           187|     Killylane|           NaN|       Redhill|
|0004|    treating_dsl.csv|2025-07-01 13:57:58|           Essex|SB2H 0GC|           117|  Craigatempin|           NaN|   Wednesfield|
|0005|mariah_substantia...|2025-07-01 13:57:58|Northamptonshire|EJ6S 6BQ|           176|     Granshagh|           NaN|     Kirkcaldy|
+----+--------------------+-------------------+----------------+--------+--------------+--------------+--------------+--------------+
only showing top 5 rows

+----+----------+---------+-----+------+----------+---------+--------------+--------------+--------+
|  ID|First_Name|Last_Name|Title|   Sex|       DOB|Ethnicity|Address_Line_1|Address_Line_2|Postcode|
+----+----------+---------+-----+------+----------+---------+--------------+--------------+--------+
|0001| Cornelius| Ferguson|  Mr.|  Male|23-04-1996|    Mixed|           190|   Invergourie|FN0F 6OF|
|0002|       Tom|     Hunt|  Sir|  Male|12-02-1973|     null|            57|  Druminiskill|TE4H 2TC|
|0003|   Kenisha|    James| Mrs.|Female|28-02-1962|    White|           187|     Killylane|PU7N 9JO|
|0004|    Manuel|   Mosley|  Sir|  Male|09-07-1984|    Mixed|           117|  Craigatempin|SB2H 0GC|
|0005|    Tyrone|      Gay|  Mr.|  Male|15-11-1975|    Black|           176|     Granshagh|EJ6S 6BQ|
+----+----------+---------+-----+------+----------+---------+--------------+--------------+--------+
only showing top 5 rows
```
````

### 7. Best practices

* Set a random seed for reproducibility.
* Choose the appropriate locale for your use case.
* Combine multiple providers for richer data.
* For large datasets, use PySpark or batch processing.
* Refer to the Faker documentation and Mimesis documentation for more features. 


### 8. Conclusion
Synthetic data generation is a powerful tool for testing, privacy protection, and simulation. Both Faker and Mimesis offer extensive capabilities for generating realistic, diverse data in Python. By using parallel examples, you can choose the library that best fits your needs and easily adapt your code for different scenarios.

We have demonstrated:
- The core concepts of providers and locales.
- How to generate a wide range of data types, including text, user profiles, and population data.
- How to scale up data generation using PySpark.
- Best practices for reproducibility and data realism.

Feel free to expand on these examples and adapt them to your own projects.

### References and further reading
* [Faker Documentation (Python)](https://faker.readthedocs.io/en/master/)
* [Mimesis API](https://mimesis.name/v12.1.1/api.html)
* [Medium Blog on Mimesis](https://medium.com/@tubelwj/mimesis-a-python-library-for-generating-test-sample-data-7809d894cbd9)
* [Getting Started with Mimesis: A Modern Approach to Synthetic Data Generation](https://www.statology.org/getting-started-mimesis-modern-approach-synthetic-data-generation/)
* [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
* [Synthpop: Synthetic Data in R](../ancillary-topics/synthpop_with_r)