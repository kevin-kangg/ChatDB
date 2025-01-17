README



Directory Structure

DSCI551\_Final\_Project

└── DSCI551\_README.pdf [README file that details how to get started with the ChatDB system.]

└── src [Directory that contains scripts and requirements.txt file.]

 ├── MongoDBCodeGenerator.py [Script that implements the MongoDB natural language query translation.]

 ├── SQLCodeGenerator.py [Script that implements the SQL natural language query translation.]

 ├── convert\_csv\_to\_json.py [Utility script that converts CSV rows to JSON documents.]

 ├── data\_set\_processor.py [Script that loads data to the databases and retrieves schema information.]

 ├── frontendv7.1.py [Script that provides the user interface.]

 ├── query\_executor.py [Script that executes and retrieves results from the database instances.]

 ├── requirements.txt [Packages that must be installed prior to executing the scripts.]

 └── sample\_queries.py [Script that contains natural language queries that have been filtered.]

└── data [Directory that contains CSV data for MySQL and JSON data for MongoDB.]

 ├── loan.csv

 ├── loan.json

 ├── purchases.csv

 ├── purchases.json

 ├── salaries.csv

 └── salaries.json





Environment

This is the environment configuration that must be configured prior to executing this project.

Python

This project assumes that the Python environment is set up according to:



Version: 3.11.10

The packages in the requirements.txt file found in the project directory are installed with the command: 

pip install -r /path/to/requirements.txt

NOTE: The packages that are most important to replicate this project are validated using this command



Database Configuration

This project assumes that the following database systems are installed:



MySQL, Version: 8.0.39

MongoDB, Version: 8.0.1

MySQL Configuration

This project assumes that the MySQL database system is configured using the following commands.



Create a user:



CREATE USER 'dsci551user'@'localhost' IDENTIFIED WITH mysql\_native\_password BY 'Dsci-Project';



Create a database:



CREATE DATABASE dsci551project;



Grant privileges to user:



GRANT CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, SELECT ON dsci551project.* TO 'dsci551user'@'localhost' WITH GRANT OPTION;



Flush the privileges:



FLUSH PRIVILEGES;



Restart the database

Data Sets

The data sets that were used (and altered) for this project are:



Loan Approval Classification





Data Science Salaries 





Predict Customer Purchase Behavior



Data Format Assumptions

MySQL assumptions: 

The name of the file will be the name of the table.

The first row in a CSV file contains the column names.

The column names with a suffix of '\_id' is a primary key (NOT SUPPORTED).

If a table with the same name is uploaded more than once, then the current table will be replaced with new table information.

MongoDB assumptions:

The JSON file is formatted as:

 {

 "Table\_Name": [

 {

 "Feature1" : Data1,

 …

 "FeatureN": DataN

 },

 …

 ]

 }

The name of the collection is the name of the file.





ChatDB

This is the procedure to start and execute the ChatDB system.

Assumptions

The MySQL database is running.

The MongoDb database is running.

The user is executing the following commands in the same directory as the source code.

The following data files are in the 'data' folder:

purchases.csv, purchases.json

salaries.csv, salaries.json

loan.csv, loan.json

User Interface

To run the ChatDB user interface, execute the following command:

streamlit run frontendv7.1.py



This will start a web browser that looks like this:



Load Data

To load data into the MySQL database, scroll to the bottom of the user interface to the "Load Data into Database". Verify that MySQL is selected in the "Select Database for Data Load":





Click the "Browse files" button:





Choose a data set that has a ".csv" suffix and click "Upload" (or similar on other systems). For this example, 'loan.csv' is selected:





Click "Load Data", the following message signals success:



Follow steps 3-6 to load all '.csv' files.

Refresh the browser, as necessary.

These instructions are analogous for MongoDB (with '.json' files replacing '.csv' files).

View SQL Data

To explore the information that has been previously loaded to the MySQL database, scroll to the "Explore Database" section and verify that SQL is selected as the database type:





Click the "Select a Table" drop-down menu and click on a table name. For example, this shows a snapshot of the "salaries" table in the database (use the horizontal scrollbar to view all columns):





NOTE: There are options to save the information, search for information in the snapshot and maximize the view that are available as icons when scrolling over the sample table..



View MongoDB Data

After loading the '.json' files successfully into the database. Scroll to the "Explore Database" section and from the "Select Database Type to Explore" drop-down menu, choose "NoSQL".





The "Available Collections" menu will list the collections in the MongoDB database. For example, selecting the "salaries" collection will display the top document returned by the find query in JSON format:





NOTE: There are options available to copy the document to the clipboard for saving to local disk.



Generate Random Queries

To generate a natural language query example, scroll to the "Sample Query" section:



Click the "Generate Sample Query" button to have a new query show in the text area above.

NOTE: There is an option to copy to the clipboard when hovering over the text area.



Execute Natural Language Queries

To generate an SQL query from a natural language query representation, scroll down to the "Generate Query from Natural Language" section:



Enter a natural language query based on the constructs defined in the project report.

Alternatively, copy a query from the "Sample Query" text box and paste it in the "Enter your natural language query" text area.

For example, copy this natural language query: "pick age and person\_income from purchases where income\_class is middle sort by person\_income descending".



Click the "Generate Query" button. 



To view the results of the query, click the "Run Query" button.



Alternatively, generate a MongoDB query by selecting "MongoDB" from the "Select Query Type" drop-down menu.

Click the "Generate Query" button:



Click the "Run Query" button to view the results of the query:



![Image 1](images/image_1.png)

![Image 2](images/image_2.png)

![Image 3](images/image_3.png)

![Image 4](images/image_4.png)

![Image 5](images/image_5.png)

![Image 11](images/image_11.png)

![Image 13](images/image_13.png)

![Image 16](images/image_16.png)

![Image 17](images/image_17.png)

![Image 18](images/image_18.png)

![Image 19](images/image_19.png)

![Image 20](images/image_20.png)

![Image 21](images/image_21.png)

![Image 22](images/image_22.png)

![Image 23](images/image_23.png)

![Image 24](images/image_24.png)

![Image 25](images/image_25.png)