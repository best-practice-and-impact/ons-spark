## Mimesis: Synthetic Data in Python

### 1. Introduction to Mimesis

Synthetic data is artificially generated information that mimics real-world data. It is commonly used for testing, model validation, training machine learning models, and protecting sensitive information, as it allows users to work with realistic datasets without exposing actual personal or confidential data.

Mimesis is a Python library used to generate fake, random, or synthetic data for various purposes as listed above. It allows users to generate a wide range of data types, including personal information, addresses, financial data, dates, and more. It is similar to [`Faker`](../ancillary-topics/faker.html), an alternative Python package used for generating synthetic data.

### 2. Install and initialise Mimesis
To start, we will need the `mimesis` library to generate the dummy data. Later on, we will use PySpark for working with Spark DataFrames in Python.
````{tabs}
```{code-tab} py
# Install Mimesis and check the installed version
%pip install mimesis
import mimesis
print(mimesis.__version__)
from mimesis import Generic
```
````
**Set up a global random seed**

You can set a global seed for all data providers and use it without explicitly passing it to each provider. Setting this seed allows us to generate the exact same synthetic data as will be shown in our examples. This is especially helpful when we are debugging or creating tests that require consistent data.
````{tabs}
```{code-tab} py
from mimesis import random

random.global_seed = 42
```
````
### 3. How Mimesis works


Mimesis, inspired by the ancient Greek concept of `"mimesis"` (which means to imitate or replicate), is designed to create realistic synthetic data rather than just random values. Its goal is to generate contextually appropriate data that mimics real-world information. This is achieved through a structured provider system and built-in support for different locales, ensuring that the generated data is both varied and meaningful.

Let us walk through how to generate basic synthetic data using a seeded instance of Mimesis. We will then generate and display basic personal and financial information. Note that the `Loacale` and `Provider` which are integral part how mimesis work are introduced here but will  explained in the next sections.

### 3. How Mimesis works

Mimesis is a Python library for generating realistic synthetic data that mimics real-world information. It uses a system of providers and locale support to create varied, contextually appropriate data for testing and development.


**What are providers and locales?**   
A `provider` in Mimesis is a class that generates a specific type of data, such as names, addresses, or dates. A `locale` specifies the regional or language settings that influence the format and style of the generated data (for example, UK vs. US address formats).



Below, we walk through how to generate basic synthetic data using a seeded instance of Mimesis. This example demonstrates how to create and display basic personal and financial information. The concepts of *Provider* and *Locale* are introduced here and will be explained in more detail in the following sections.
````{tabs}
```{code-tab} py
# The following example demonstrates how to generate basic personal and financial data using the UK locale (en_GB) and a fixed seed for reproducibility.
from mimesis import Person, Address, Finance
from mimesis.locales import Locale

person = Person(locale=Locale.EN_GB, seed=42)
address = Address(locale=Locale.EN_GB, seed=42)
finance = Finance(locale=Locale.EN_GB, seed=42)

print("Name:", person.full_name())
print("Email:", person.email())
print("Address:", address.address())
print("Job:", person.occupation())
print("Company:", finance.company())
```
````

```plaintext
Name: Anthony Reilly
Email: holds1871@live.com
Address: 1310 Blaney Avenue
Job: Choreographer
Company: Centrica
```
The outputs produced by Mimesis appear realistic and reflect real-world naming and formatting conventions.

### 4. Mimesis provider system

One of the key strengths of Mimesis is its well-structured provider system. At its core is the **Generic provider**, which acts as a central hub, giving you access to all the specialised data generators available in Mimesis. Let us begin by exploring the range of providers that you can use:
````{tabs}
```{code-tab} py
from mimesis import Generic
from mimesis.locales import Locale

generic = Generic(locale=Locale.EN_GB, seed=42)

print("Providers available through Generic:")
for attribute in dir(generic):
    if not attribute.startswith('_'):  # Here, we skip internal attributes
        print(attribute)

```
````

```plaintext
Providers available through Generic:
address
binaryfile
choice
code
cryptographic
datetime
development
file
finance
food
hardware
internet
numeric
path
payment
person
science
text
transport
```
### 5. Locale support for international applications

A key feature of Mimesis is its ability to handle different locales. When generating test data for global applications, it is essential that the data reflects not only different languages but also the cultural and formatting norms of various regions. Mimesis achieves this through its robust locale system.

Now, let us see how Mimesis adjusts its data generation based on different locale settings. For a full list of supported locales, please refer to the [official Mimesis documentation](https://mimesis.name/v12.1.1/locales.html).


To demonstrate how locales influence data generation, let us create a simple example that generates person and address data for various regions. We will use a fixed seed to ensure consistent results. Note that Mimesis also supports language-specific data generation; for example, outputs for AR-EG and JA locales are written in the respective languages.
````{tabs}
```{code-tab} py
from mimesis import Person
from mimesis.locales import Locale

examples = {}

# List of diverse locales to generate data for
locales = [Locale.EN_GB, Locale.EN, Locale.JA, Locale.FR, Locale.AR_EG]

 
for locale in locales:
    person = Person(locale=locale, seed=42)
    examples[locale] = {
        "Full Name": person.full_name(),
        "Phone": person.telephone(),
        "Email": person.email(),
        "Job": person.occupation()
    }

for locale, data in examples.items():
    print(f"\n{locale.value.upper()} Examples:")
    for key, value in data.items():
        print(f"{key}: {value}")
```
````

```plaintext

EN-GB Examples:
Full Name: Anthony Reilly
Phone: 055 2768 0402
Email: appeared1901@example.org
Job: Veterinary Surgeon

EN Examples:
Full Name: Anthony Reilly
Phone: +1-309-276-8040
Email: guitars1813@yahoo.com
Job: Yacht Master

JA Examples:
Full Name: 石松 田場
Phone: +81 117 5500 2657
Email: readers2029@example.org
Job: レコーディング・エンジニア

FR Examples:
Full Name: Alexy Rigal
Phone: 0427680402
Email: appeared1901@example.org
Job: Responsable de la promotion des ventes

AR-EG Examples:
Full Name: أيمن عبد الماجد باشا
Phone: 0611755002
Email: water2079@duck.com
Job: مهندس تنظيف
```
### 6. Common Mimesis providers

Mimesis offers a wide range of providers for generating different types of data. Some of the most commonly used providers include:

- **Person**: Generates names, genders, emails, phone numbers, and other personal information.
- **Address**: Produces realistic street addresses, cities, postcodes, and regions.
- **Finance**: Generates financial data such as bank names, company names, currency codes, and prices.
- **Datetime**: Produces random dates, times, and timestamps.
- **Text**: Generates random sentences, words, and quotes.

There are many additional providers available in Mimesis for generating data such as transport, science, food, and more. For a full list and detailed breakdowns, please refer to the [official Mimesis documentation](https://mimesis.name/v12.1.1/providers.html).

**Class: mimesis.Text**

The `mimesis.Text` class generates random text data, such as sentences, words, quotes, levels, answers, and so on.

**Key methods in `mimesis.Text`**
````{tabs}
```{code-tab} py
# Import the necessary classes from Mimesis
from mimesis import Generic
from mimesis.locales import Locale

# Initialize the Generic provider with UK locale and a fixed seed for reproducibility
generic = Generic(locale=Locale.EN_GB, seed='OK')

# --- Section 1: Random Sentence ---
print("### 1. Random Sentence")

# Generate a random sentence
random_sentence = generic.text.sentence()
print(f"Random Sentence: {random_sentence}")

# --- Section 2: Random Word ---
print("\n### 2. Random Word")

# Generate a random word
random_word = generic.text.word()
print(f"Random Word: {random_word}")

# --- Section 3: Random Text (Multiple Sentences) ---
print("\n### 3. Random Text")

# Generate random text consisting of multiple sentences
random_text = generic.text.text()
print(f"Random Text (multiple sentences):\n{random_text}")

# --- Section 4: Random Quote ---
print("\n### 4. Random Quote")

# Generate a random quote (could be used for example datasets in surveys or quotes sections)
random_quote = generic.text.quote()
print(f"Random Quote: {random_quote}")

# --- Section 5: Random Answer  ---
print("\n### 5. An Answer")

# Generates a random answer in the current language
random_answer = generic.text.answer()
print(f"Random Answer:\n{random_answer}")

# --- Section 6: Random Level ---
print("\n### 6. Random Level")

# Generates a word that indicates a level of something
random_level = generic.text.level()
print(f"Random Level:\n{random_level}")

# --- Section 7: Random Long Paragraphs ---
print("\n### 6. Random Paragraph")

# Generates a long paragraph from sentences. Specify the length of the list of sentences
random_sentences = [generic.text.text() for _ in range(10)]
long_paragraph = " ".join(random_sentences)
print(f"Random Long Paragraph:\n{long_paragraph}")


```
````

```plaintext
### 1. Random Sentence
Random Sentence: Messages can be sent to and received from ports, but these messages must obey the so-called "port protocol."

### 2. Random Word
Random Word: checkout

### 3. Random Text
Random Text (multiple sentences):
Haskell features a type system with type inference and lazy evaluation. They are written as strings of consecutive alphanumeric characters, the first character being lowercase. She spent her earliest years reading classic literature, and writing poetry. Make me a sandwich. He looked inquisitively at his keyboard and wrote another sentence.

### 4. Random Quote
Random Quote: Those who refuse to learn from history are condemned to repeat it.

### 5. An Answer
Random Answer:
Yes

### 6. Random Level
Random Level:
high

### 6. Random Paragraph
Random Long Paragraph:
Do you come here often? I don't even care. Haskell features a type system with type inference and lazy evaluation. Haskell is a standardized, general-purpose purely functional programming language, with non-strict semantics and strong static typing. Tuples are containers for a fixed number of Erlang data types. Ports are used to communicate with the external world. The syntax {D1,D2,...,Dn} denotes a tuple whose arguments are D1, D2, ... Dn. He looked inquisitively at his keyboard and wrote another sentence. Messages can be sent to and received from ports, but these messages must obey the so-called "port protocol." Its main implementation is the Glasgow Haskell Compiler. Messages can be sent to and received from ports, but these messages must obey the so-called "port protocol." Messages can be sent to and received from ports, but these messages must obey the so-called "port protocol." Messages can be sent to and received from ports, but these messages must obey the so-called "port protocol." Tuples are containers for a fixed number of Erlang data types. I don't even care. Erlang is a general-purpose, concurrent, functional programming language. Ports are used to communicate with the external world. Type classes first appeared in the Haskell programming language. Initially composing light-hearted and irreverent works, he also wrote serious, sombre and religious pieces beginning in the 1930s. The arguments can be primitive data types or compound data types. In 1989 the building was heavily damaged by fire, but it has since been restored. The Galactic Empire is nearing completion of the Death Star, a space station with the power to destroy entire planets. Its main implementation is the Glasgow Haskell Compiler. Erlang is a general-purpose, concurrent, functional programming language. I don't even care. Haskell features a type system with type inference and lazy evaluation. They are written as strings of consecutive alphanumeric characters, the first character being lowercase. Initially composing light-hearted and irreverent works, he also wrote serious, sombre and religious pieces beginning in the 1930s. Haskell is a standardized, general-purpose purely functional programming language, with non-strict semantics and strong static typing. In 1989 the building was heavily damaged by fire, but it has since been restored. Do you come here often? Tuples are containers for a fixed number of Erlang data types. Ports are used to communicate with the external world. In 1989 the building was heavily damaged by fire, but it has since been restored. Atoms can contain any character if they are enclosed within single quotes and an escape convention exists which allows any character to be used within an atom. Haskell is a standardized, general-purpose purely functional programming language, with non-strict semantics and strong static typing. The syntax {D1,D2,...,Dn} denotes a tuple whose arguments are D1, D2, ... Dn. The Galactic Empire is nearing completion of the Death Star, a space station with the power to destroy entire planets. Type classes first appeared in the Haskell programming language. Its main implementation is the Glasgow Haskell Compiler. Its main implementation is the Glasgow Haskell Compiler. Atoms can contain any character if they are enclosed within single quotes and an escape convention exists which allows any character to be used within an atom. Atoms are used within a program to denote distinguished values. He looked inquisitively at his keyboard and wrote another sentence. The arguments can be primitive data types or compound data types. They are written as strings of consecutive alphanumeric characters, the first character being lowercase. Its main implementation is the Glasgow Haskell Compiler. Initially composing light-hearted and irreverent works, he also wrote serious, sombre and religious pieces beginning in the 1930s. The syntax {D1,D2,...,Dn} denotes a tuple whose arguments are D1, D2, ... Dn. Erlang is known for its designs that are well suited for systems.
```
### 7. Generating fake datasets wit Spark

In this section, we will use PySpark along with Mimesis to generate synthetic person, finance, and population data, utilising Spark DataFrames instead of Pandas DataFrames. However, you can use Pandas DataFrames if that better suits your needs. PySpark allows you to scale the data generation process and handle larger datasets in a distributed manner.

#### 7.1 Generating fake person data

Here, the code uses the Mimesis library to generate synthetic personal data, such as names, genders, ages, emails, phone numbers, nationalities, and occupations. By initialising the Person object with the English (GB) locale with a seed for reproducibility, the code generates `n` number of rows (`n_rows`) of data, with each row containing randomly generated values for various personal attributes. The generated data is then passed on to a Spark dataframe for maniplation.

If you encounter problem creating a Spark DataFrame using `spark.createDataFrame()` while runing in a virtual enviroment (venv). The code below will handle the issue.

* If you're not in a virtual environment, the code directly uses `spark.createDataFrame()` to create the Spark DataFrame.  
* If you're in a virtual environment, the data is first generated, then saved to a CSV file, and finally read into a Spark DataFrame using `spark.read.csv()`. 

To determine if you're in a virtual environment, the code checks whether the `VIRTUAL_ENV` environment variable is set.
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
from mimesis import Person, Locale
from mimesis import Generic
import pandas as pd
import os

spark = SparkSession.builder.master("local[2]").appName("MimesisDataGeneration").getOrCreate()

# Check if in virtual environment
is_in_venv = os.getenv('VIRTUAL_ENV') is not None

person = Person(locale=Locale.EN_GB, seed=42)

n_rows = 10
personal_data = {
    "First Name": [person.first_name() for _ in range(n_rows)],
    "Last Name": [person.last_name() for _ in range(n_rows)],
    "Full Name": [person.full_name() for _ in range(n_rows)],
    "Gender": [person.gender() for _ in range(n_rows)],
    "Age": [person.random.randint(16, 88) for _ in range(n_rows)],
    "Email": [person.email() for _ in range(n_rows)],
    "Phone Number": [person.phone_number() for _ in range(n_rows)],
    "Nationality": [person.nationality() for _ in range(n_rows)],
    "Occupation": [person.occupation() for _ in range(n_rows)],
}

if not is_in_venv:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: spark.createDataFrame")
    personal_df = spark.createDataFrame(
        [(v) for v in zip(*personal_data.values())],  
        schema=list(personal_data.keys())  
    )

else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame")
    personal_df_pd = pd.DataFrame(personal_data)

    file_path = "personal_data.csv"
    personal_df_pd.to_csv(file_path, index=False)
    personal_df = spark.read.csv(file_path, header=True, inferSchema=True)
     
personal_df.limit(10).show()
```
````
**Note:** If you encounter an error when creating a Spark DataFrame in a virtual environment, use the workaround provided (generate data, save as CSV, and read with `spark.read.csv()`). This is the same issue and solution as described in the Faker notebook.

#### 7.2 Generating fake finance data

In this section, the code generates synthetic finance-related data using the Mimesis library's `Finance` class. The data includes bank names, company names and types, cryptocurrency data (ISO codes and symbols), currency data, stock exchange names, stock tickers, and random prices. This is done for n_rows rows, with the data organised in a Spark DataFrame. The locale is set to English (GB) to generate finance-related data that follows British conventions, with a seed for reproducibility.
````{tabs}
```{code-tab} py
from mimesis import Finance, Locale

is_in_venv = os.getenv('VIRTUAL_ENV') is not None

finance = Finance(locale=Locale.EN_GB, seed=42)

n_rows = 1000

financial_data = {
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
    finance_df = spark.createDataFrame(
        [(v) for v in zip(*financial_data.values())],  
        schema=list(financial_data.keys())  
    )

else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame")
    finance_df_pd = pd.DataFrame(financial_data)
    finance_df_pd.to_csv("finance_data.csv", index=False)
    finance_df = spark.read.csv("finance_data.csv", header=True, inferSchema=True)

finance_df.limit(10).show()
```
````

```plaintext
is_in_venv: True | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame
+--------------------+--------------+--------------------+-----------------------+---------------------+-----------------+---------------+------------+------------+-------------------+--------------------+------------+
|           Bank Name|  Company Name|        Company Type|Cryptocurrency ISO Code|Cryptocurrency Symbol|Currency ISO Code|Currency Symbol|Random Price|Price in BTC|Stock Exchange Name|          Stock Name|Stock Ticker|
+--------------------+--------------+--------------------+-----------------------+---------------------+-----------------+---------------+------------+------------+-------------------+--------------------+------------+
|National Counties...|      Glencore|Private company l...|                    VTC|                    Ξ|              GBP|              £|     1284.03|    0.291637|           Euronext|            PTC Inc.|         VMM|
|The Royal Bank of...|Derwent London|Limited Liability...|                    XBC|                    Ξ|              GBP|              £|      817.86|   0.0897551|           Euronext|Boston Omaha Corp...|         TEN|
|Royal Bank of Sco...|       Hunting|        Incorporated|                    DOT|                    Ξ|              GBP|              £|      719.96|   1.6363438|               HKEX|World Acceptance ...|       PACQU|
|Paragon Banking G...|           Dcc|         Corporation|                    BTC|                    Ξ|              GBP|              £|      683.87|   0.3993704|                SSE| Waitr Holdings Inc.|       OFSSZ|
|     Triodos Bank UK|        Booker|         Corporation|                    XRP|                    Ξ|              GBP|              £|      568.18|   0.7476416|             NASDAQ|Eaton Vance Calif...|        SRAX|
| The Bank of Ireland|      Bgeo Grp|Private company l...|                   WBTC|                    Ł|              GBP|              £|     1005.14|   1.5154676|                SSE|      InspireMD Inc.|        PTLA|
| OneSavings Bank plc|        Dunelm|        Incorporated|                    BNB|                    Ξ|              GBP|              £|      915.93|   1.7055283|                SSE|PennantPark Float...|       RUSHA|
|          Tesco Bank|   Jpmor.amer.|Limited Liability...|                   DASH|                    Ł|              GBP|              £|     1037.04|   0.2247435|           Euronext|Piedmont Office R...|         NGS|
|Penrith Building ...|   Wetherspoon| Limited Partnership|                    XBT|                    Ł|              GBP|              £|      591.95|    0.109076|             NASDAQ|VirnetX Holding Corp|        ZAGG|
|Felixstowe & Walt...|    Tullow Oil|Limited Liability...|                    IOT|                    ₿|              GBP|              £|       721.2|   1.8978818|               NYSE|International Gam...|        PETZ|
+--------------------+--------------+--------------------+-----------------------+---------------------+-----------------+---------------+------------+------------+-------------------+--------------------+------------+
```
#### 7.3 Generating fake population data

We use Mimesis's `Generic`, `Address`, and `Person` providers to generate fake population data (e.g., address, postcode, names, etc.).  
We define the function `generate_elector_data()` to create fake data for several fields like local authority, postcode, DOB, etc.  
It also include special handling to create continuity in address lines for a subset of rows and adds extra columns for address continuity.  

* **Schema Consistency**: We define the schema upfront with StructType
* **Data Generation**: The `generate_elector_data()` function is used to generate the data
* **Address Continuity**: For a subset of rows, the code ensures continuity in address lines to simulate real-world scenarios where multiple entries might share the same address.
* **Handling Virtual Environment**: The function `in_virtualenv()` checks if the `VIRTUAL_ENV` environment variable is set
    * Without Virtual Environment: We directly use PySpark’s `spark.createDataFrame()` to generate the DataFrame.
    * With Virtual Environment: We generate the data using mimesis, convert it to a pandas DataFrame, save it as a CSV, and then read it using `spark.read.csv()` with the predefined schema.
````{tabs}
```{code-tab} py
import pandas as pd
import numpy as np
from mimesis import Generic
from mimesis.locales import Locale
import datetime
import os
from mimesis.enums import TitleType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("MimesisData").getOrCreate()

def in_virtualenv():
    return os.environ.get('VIRTUAL_ENV') is not None

# Initialise mimesis Generic provider with a seed for reproducibility
data_generator = Generic(locale=Locale.EN_GB, seed=42)

# Providers
address_provider = data_generator.address
person_provider = data_generator.person

# Define the schema upfront  
schema = StructType([
    StructField("ID", StringType(), True),
    StructField("SOURCE_FILE", StringType(), True),
    StructField("TIME_STAMP", StringType(), True),
    StructField("Local_Authority", StringType(), True),
    StructField("Postcode", StringType(), True),
    StructField("Address_Line_1", StringType(), True),
    StructField("Address_Line_2", StringType(), True),
    StructField("Address_Line_3", StringType(), True),
    StructField("Address_Line_4", StringType(), True),
    StructField("Address_Line_5", StringType(), True),
    StructField("Address_Line_6", StringType(), True),
    StructField("Address_Line_7", StringType(), True),
    StructField("Address_Line_8", StringType(), True),
    StructField("Address_Line_9", StringType(), True),
    StructField("Last_Name", StringType(), True),
    StructField("Middlenames", StringType(), True),
    StructField("First_Name", StringType(), True),
    StructField("Title", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("guid", StringType(), True)
])

# Function to generate fake electoral data
def generate_elector_data(n_rows):
    data = {
        "Local_Authority": [],
        "Postcode": [],
        "Address_Line_2": [],
        "Address_Line_3": [],
        "Address_Line_4": [],
        "Address_Line_5": [],
        "Last_Name": [],
        "Middlenames": [],
        "First_Name": [],
        "Title": [],
        "guid": [],
        "SOURCE_FILE": [],
        "Address_Line_1": [],
        "DOB": [],
        "TIME_STAMP": []
    }

    for i in range(n_rows):
        # Generate address and personal data
        data["Local_Authority"].append(address_provider.region())
        data["Postcode"].append(address_provider.postal_code())
        data["Address_Line_2"].append(address_provider.street_name())
        data["Address_Line_3"].append(address_provider.city())
        data["Address_Line_4"].append(address_provider.city())
        data["Address_Line_5"].append(address_provider.country())
        
        # Generate names
        data["Last_Name"].append(person_provider.last_name())
        data["Middlenames"].append(person_provider.name())
        data["First_Name"].append(person_provider.name())
        data["Title"].append(person_provider.title(title_type=TitleType.TYPICAL))
        
        # Unique GUID for each entry
        data["guid"].append(person_provider.password())
        
        # Source file, placeholder for now
        data["SOURCE_FILE"].append(f'{person_provider.username(mask="l_l")}.csv')
        
        # Address line 1 - Random number as address number
        data["Address_Line_1"].append(person_provider.random.randint(1, 200))

        # Date of Birth: Randomly generate between ages 18-99
        dob_year = person_provider.random.randint(1925, 2007)
        dob_month = person_provider.random.randint(1, 12)
        dob_day = person_provider.random.randint(1, 28)  # Using 28 here due to February having only 28 days
        data["DOB"].append(f"{dob_day:02d}-{dob_month:02d}-{dob_year}")
        
        # Timestamp of data generation
        data["TIME_STAMP"].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    return data

# Generate fake data
n_rows = 1000
data = generate_elector_data(n_rows)

# Create a DataFrame based on the generated data
df = pd.DataFrame(data)

# Update some columns as a copy for certain rows
same_street_rows = 10
for i in range(same_street_rows):
    df.at[n_rows - (i + 1), 'Address_Line_4'] = df.at[n_rows - 1, 'Local_Authority']
    df.at[n_rows - (i + 1), 'Address_Line_3'] = df.at[n_rows - 1, 'Address_Line_3']
    df.at[n_rows - (i + 1), 'Address_Line_2'] = df.at[n_rows - 1, 'Address_Line_2']
    df.at[n_rows - (i + 1), 'Postcode'] = df.at[n_rows - 1, 'Postcode']
    df.at[n_rows - (i + 1), 'SOURCE_FILE'] = df.at[n_rows - 1, 'SOURCE_FILE']

# Adding extra columns for address continuity
df['Address_Line_6'] = np.nan
df['Address_Line_7'] = np.nan
df['Address_Line_8'] = np.nan
df['Address_Line_9'] = np.nan

# Add ID column starting from 1
df['ID'] = [str(i).zfill(len(str(n_rows))) for i in np.arange(1, n_rows + 1)]

df = df[[
    'ID', 'SOURCE_FILE', 'TIME_STAMP', 'Local_Authority', 'Postcode', 'Address_Line_1', 'Address_Line_2',
    'Address_Line_3', 'Address_Line_4', 'Address_Line_5', 'Address_Line_6', 'Address_Line_7', 'Address_Line_8',
    'Address_Line_9', 'Last_Name', 'Middlenames', 'First_Name', 'Title', 'DOB', 'guid'
]]

# Now either directly use PySpark or read from the CSV depending on the environment
if in_virtualenv():
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame")
    file_path = "electoral_data.csv"
    df.to_csv(file_path, index=False)
    population_df = spark.read.csv(file_path, header=True, inferSchema=True)
    population_df.show()
else:
    print(f"is_in_venv: {is_in_venv} | Spark DataFrame to be created from: spark.createDataFrame")
    pd.DataFrame.iteritems = pd.DataFrame.items
    population_df = spark.createDataFrame(df)
    population_df.show()
```
````

```plaintext
is_in_venv: True | Spark DataFrame to be created from: Pandas DataFrame -> CSV -> Spark DataFrame
+---+--------------------+-------------------+--------------------+--------+--------------+--------------+--------------+--------------+--------------------+--------------+--------------+--------------+--------------+----------+-----------+----------+------+----------+-----------+
| ID|         SOURCE_FILE|         TIME_STAMP|     Local_Authority|Postcode|Address_Line_1|Address_Line_2|Address_Line_3|Address_Line_4|      Address_Line_5|Address_Line_6|Address_Line_7|Address_Line_8|Address_Line_9| Last_Name|Middlenames|First_Name| Title|       DOB|       guid|
+---+--------------------+-------------------+--------------------+--------+--------------+--------------+--------------+--------------+--------------------+--------------+--------------+--------------+--------------+----------+-----------+----------+------+----------+-----------+
|  1| appeared_fetish.csv|2025-04-04 13:43:51|  County Londonderry|AH2T 6XC|           184|        Egeria|      Barrhead|        Barnet|               Benin|          NULL|          NULL|          NULL|          NULL|   Mullins|    Anthony|     Kaley|   Mr.|08-07-1994|"(""@iNcuV"|
|  2|security_councils...|2025-04-04 13:43:51|            Grampian|GP5S 7KL|            97|    Craigatoke|     Aldeburgh|Ellesmere Port|            Portugal|          NULL|          NULL|          NULL|          NULL|      Hunt|      Aleen|   Neville|  Miss|10-09-1935|   o`Fij<4.|
|  3|intended_elementa...|2025-04-04 13:43:51|     South Glamorgan|IE9I 0CW|           137|      Keadybeg|     Liverpool|   Berkhamsted|          San Marino|          NULL|          NULL|          NULL|          NULL| Singleton|     Harold|    Benito|   Sir|15-03-1956|   }=>JQ;pH|
|  4|secretary_instant...|2025-04-04 13:43:51|             Suffolk|NZ3O 8QW|            36|        Hornby|     Guildford| Brightlingsea|               Aruba|          NULL|          NULL|          NULL|          NULL|    Harmon|       Jaye|   Britany|   Mr.|18-12-1956|   .Lg\1&tU|
|  5|photographs_toys.csv|2025-04-04 13:43:51|               Gwent|UZ8W 3LV|            65|        Canary|     Rostrevor|     Prestatyn|    Congo - Kinshasa|          NULL|          NULL|          NULL|          NULL|     Lyons|    Roberto|    Melvin|   Mr.|22-01-1995|   Viek6-Nf|
|  6| lightning_fever.csv|2025-04-04 13:43:51|Highlands and Isl...|SR0Q 5GM|            40|    Corraquill|          Hove|   Northampton|              Sweden|          NULL|          NULL|          NULL|          NULL|     Pratt|      Terry|   Margart|   Sir|18-03-1972|   OQ`?yVq\|
|  7|   bucks_couples.csv|2025-04-04 13:43:51|           Cleveland|FA3G 2YW|            33|         Dalys|      Hinckley|     Lichfield|             Hungary|          NULL|          NULL|          NULL|          NULL|Williamson|     Kareem|   Angeles|   Sir|06-09-1985|   ?;Cf?`iT|
|  8|     buried_gray.csv|2025-04-04 13:43:51|             Suffolk|DD7O 7LP|            18|    Drumagrove|          Hove|        Didcot|     Macau SAR China|          NULL|          NULL|          NULL|          NULL|  Espinoza|   Jacqulyn|       Jon|  Miss|03-06-1929|   PWlvF3vu|
|  9|  recruiting_she.csv|2025-04-04 13:43:51|      West Glamorgan|CB1Q 7KB|           187|    Drumleacht|       Wigtown|         Largs|      Western Sahara|          NULL|          NULL|          NULL|          NULL|     Leach|      Jaime|    Jamika|Master|21-11-1931|   ^1S,.rjO|
| 10|venice_beginning.csv|2025-04-04 13:43:51|        Bedfordshire|RC8T 7IH|           167|       Calmore|Wellingborough|   Abertillery|        Vatican City|          NULL|          NULL|          NULL|          NULL|      Neal|      Brain| Margareta|   Mr.|03-01-1994|   sYnrR?h.|
| 11|vitamins_dispute.csv|2025-04-04 13:43:51|       Hertfordshire|ZT5C 6VN|            49|   Castlekeele|       Dursley|       Rugeley|            Slovenia|          NULL|          NULL|          NULL|          NULL|   Watkins|      Earle|    Sunday|   Ms.|02-04-1962|   >[paK^,A|
| 12|witch_australian.csv|2025-04-04 13:43:51|Dumfries and Gall...|OU5A 3AY|           159|    Cunningham|    Killyleagh|        Bootle|       Côte d’Ivoire|          NULL|          NULL|          NULL|          NULL| Mcfarland|   Jonathon|     Berry|   Ms.|22-07-1935|   fV=gg>L_|
| 13|        coat_tub.csv|2025-04-04 13:43:51|       County Antrim|CM0T 7DM|            55|    Hannahglen|    Failsworth|     Llandeilo|            Maldives|          NULL|          NULL|          NULL|          NULL|  Mcintosh|      Keila|    Jordan|   Sir|05-05-1989|"L""CD)gR~"|
| 14|combining_install...|2025-04-04 13:43:51|     South Glamorgan|ZY7R 7KR|            30|      Drumearn|    Trowbridge|       Warwick|British Indian Oc...|          NULL|          NULL|          NULL|          NULL|     Weeks|     Caroll|   Luciano|   Ms.|18-12-1938|   PZC}.X!0|
| 15|accreditation_cup...|2025-04-04 13:43:51|           Hampshire|FI5F 2BQ|           164|     Clonallan|   Bridlington|    Barnstaple|          Tajikistan|          NULL|          NULL|          NULL|          NULL| Cervantes|      Lanie|     Lance|Master|24-03-1958|   7yT\<iNA|
| 16|authentic_former.csv|2025-04-04 13:43:51|          Merseyside|BG6F 1YO|           175|      Galbally|       Kinross|        Witney|   St. Kitts & Nevis|          NULL|          NULL|          NULL|          NULL|     Huber|      Alica|     Byron|Master|04-11-1956|   dIZOdI^||
| 17|completely_attrib...|2025-04-04 13:43:51|          Shropshire|EC4L 4SR|           121|      Kinnegar|    Chelmsford|     Bracknell|               India|          NULL|          NULL|          NULL|          NULL|     Greer|       Enda|    Duncan|   Sir|27-04-1953|   c(F^.>/z|
| 18|trips_appointment...|2025-04-04 13:43:51|       Mid Glamorgan|UC1N 1EM|            30|      Bankhall|       Verwood|     Chatteris|    Ascension Island|          NULL|          NULL|          NULL|          NULL|   Walters|  Marquetta|    Jessia|   Mr.|19-03-1958|  !LAg+HV""|
| 19| tunisia_outputs.csv|2025-04-04 13:43:51|        Bedfordshire|ZY8E 4FK|           111|   Ballynaloan|      Eyemouth|       Salford|         Afghanistan|          NULL|          NULL|          NULL|          NULL|      Wood|    Aundrea|    Oliver|  Miss|11-11-1933|   P}k[reOW|
| 20| wiring_properly.csv|2025-04-04 13:43:51|         Oxfordshire|GU4K 9ZO|           141| Lisnascreghog|    Birmingham|    Eastbourne|    Christmas Island|          NULL|          NULL|          NULL|          NULL|     Moore|      Detra|    Thresa| Madam|10-05-1925|   E$0s!J)q|
+---+--------------------+-------------------+--------------------+--------+--------------+--------------+--------------+--------------+--------------------+--------------+--------------+--------------+--------------+----------+-----------+----------+------+----------+-----------+
only showing top 20 rows
```
### 8. Conclusion

In this notebook, we explored how to generate synthetic data using the `mimesis` library. We covered various classes, including `Person`, `Datetime`, `Finance`, `Address`, and `Transport`. These classes offer a rich set of features to generate realistic data for testing machine learning models or simulating real-world datasets. 

The `mimesis` library provides a flexible and powerful way to create diverse types of synthetic data, ensuring that the generated data is contextually appropriate and varied. By leveraging different locales, we can generate data that reflects the cultural and formatting norms of various regions, making it suitable for international applications.

Additionally, we demonstrated how to integrate `mimesis` with PySpark to handle larger datasets in a distributed manner, showcasing the scalability of the data generation process.

Feel free to explore and modify the code to suit your data generation needs. This notebook can be expanded with additional classes, more detailed data generation examples, and use cases, depending on the specific needs of the users.

### References


* [Mimesis API](https://mimesis.name/v12.1.1/api.html)
* [Medium Blog on Mimesis](https://medium.com/@tubelwj/mimesis-a-python-library-for-generating-test-sample-data-7809d894cbd9)
* [Getting Started with Mimesis: A Modern Approach to Synthetic Data Generation](https://www.statology.org/getting-started-mimesis-modern-approach-synthetic-data-generation/)
* [Synthpop: Syntheic Data in R](../ancillary-topics/synthpop_with_r)
* [Faker: Synthetic Data in Python](../ancillary-topics/faker)