## "MULTINATIONAL RETAIL DATA CENTRALISATION"

The project automates a pipline the process of centralizing various data sources into a single clean database.  
Using Python, AWS services, and PostgreSQL, the pipeline extracting data from various formats, cleaning data inconsistencies, 
injection it into a database for the accessibility and reliability of data for genarating performnce reports.

The aim of the project is to experience real-world data engineering practices and gain hands-on experience.
### Chief Challenges ;
- Handling data discrepancies ; 
- integrating different data sources,
- Checking raw material before the extraction, 

### Learned ; 
- How can be ensured data integrity across different data sources and formats.
- Different connections for different data sources.
- Using appropriate packages, and libraries to clean data considering using resources effective and efficient way.
- By implementing comprehensive data validation and cleaning routines.
- The significance of data quality assurance in data warehousing.
- Awaring different sources, environments, and formats for data ( AWS RDS, S3 Bucket, API, JSON, CSV, PDF, DataFrame ) and its features.   
- Securely using credentials.
- Working in different environments and using different tools. For instance, using Python and Pandas to establish a pipeline, and gathering data to analyse it working with SQL ( pgAdmin4 ). 

### Environment ;
- Install and intiate "Git" to track the code. 
- Create a virtul environment "venv" 
- Activate the virtual environment 

### python -m venv venv
source venv\Scripts\activate
pip install -r requirements.txt

### Installed package and Libraries ;
Install the packages needed for project
- boto3==1.34.39 - connection for AWS and Services ( S3 Bucket and AWS RDS )
- botocore==1.34.39
- certifi==2024.2.2
- charset-normalizer==3.3.2
- distro==1.9.0
- greenlet==3.0.3
- idna==3.6
- jmespath==1.0.1
- JPype1==1.5.0
- numpy==1.26.4 - performing a wide variety of mathematical operations on arrays
- packaging==23.2
- pandas==2.2.0 - creating DataFrame and manupulate data
- psycopg2==2.9.9 - connection and manupulation database
- pyarrow==15.0.0
- python-dateutil==2.8.2
- pytz==2024.1
- PyYAML==6.0.1 - to read yaml file
- requests==2.31.0
- s3transfer==0.10.0
- six==1.16.0
- SQLAlchemy==2.0.26 - connection to database 
- tabula-py==2.9.0 - reading PDF data
- typing_extensions==4.9.0
- tzdata==2024.1
- urllib3==2.0.7

### Create directories and files ;
- Mult_Data_Cent -- > Directory which hold the project.
- database_utils.py -- > File holding the code which connect to source and target database. 
- data_extraction.py -- > File holding read and extract data from different sources.
- data_cleaning.py -- > File holding clean the data according to requirements.
- main.py -- > File orcestrating the execution
- Mult_Data_Cent_Database -- > Directory holding sql queries about the project. 
- db_creds.yaml -- > File holding source database credentials
- target_db_creds.yaml -- > File holding target database credentials.
- requirements.txt -- > File holding the packages, the libraries need by the project. 
- .gitignore --> File keeping senstive informations hide.

## "database_utils.py"
Learned how to connect to a source and target database creating engine using sqlalchemy in a secure way using .gitignore and credentials files like yaml.  

 
## "data_extraction.py"
Learned how to connect to S3 Bucket and API.  Then Extract data from AWS RDS, S3 Bucket and API in "df", "PDF", "CSV", "JSON". 

AWS RDS - data frame  ; 
import and use the classes and methods  from data_utils.py  connect to database and extract the data to load in data frame for preparing for cleaning.    

API - json data ;
connect to the endpoint using its credentials to list and extract the data in json then load the data in pandas df to upload the target database.      

S3 Bucket - CSV Data, JSON Data, PDF Data  ;
connect to S3 bucket using its credentials. 
Extract csv data and convert it to Pandas df to inject into the target database. 
Extract json data convert it to Pandas df to inject into the target database. 
Extract pdf data extract data, concotanate the pages, convert the data to Pandas df to inject into the target database. 

## "data_cleaning.py"

Use @staticmethod to clean each data in different format coming from various data source. 

## "main.py"

Create a flexible code using less hard coding and orchestrations using sys.argv() giving entering data source address opportunities on the terminal.

 

 




     



