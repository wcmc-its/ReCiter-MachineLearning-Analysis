ReCiter stores results from its analysis in both DynamoDB (a NoSQL database) and S3 (a file storage system). It also exposes data through its various APIs. Generally speaking, these systems are optimized for performance, so it takes a little extra effort to retrieve data, so it can be analyzed. As you can see, there are numerous ways to get data from your ReCiter instance. 

## Retrieving CSV data from DynamoDB using Python

1. Prerequisites: if you have not done so already... 
    1. Download the [Visual Studio Code application](https://code.visualstudio.com/download).
    1. Install boto3. Go to command line and enter, `pip install boto3 --user`
    1. We need to connect to DynamoDB. Install AWS command line interface as per [these directions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
    1. Configure AWS CLI. Enter access key and secret key 
1. Open DynamoDB_Analysis.py in the Visual Studio application. 
    1. This Python file contains a series of commands, 
        1. The first part is for scan the DynamoDB table and get everything, store in a Python list
        1. Other parts are for each table to be output. For example, the personArticle_mysql.csv table, will return all elements within DynamoDB at the level of the individual person / article. One such element is a PMID; another is the personTypeScore.
    1. There are currently eight tables to be output.
    1. To ignore certain tables for export from DynamoDB and comment them out, use three apostrophes before and after the code.
1. Set the outputPath equal to the location of where you wish to output the files from this script.
1. Save the file.
1. Execute the Python file:
    1. Option #1: Use the Terminal
        1. In the Terminal, navigate to the location of this Python file.
        1. Execute `python3 DynamoDB_Analysis_upload.py`
        1. The code will output a series of CSV files to the same directory as the Python file
    1. Option #2:
        1. In VisualStudio, right-click on the body of the Python file.
        1. Click on the “Run Python file in Terminal” option
        1. The code will output a series of CSV files to the same directory as the Python file
    1. Notes:
        1. In our experience, for about 9,000 people and 100,000 records, it takes approximately 10-15 minutes to perform these scans. This is true if you run all of them together or if you run them one at a time. 
        1. If your code does not properly anticipate a null key, you will see a “KeyError.” The code includes examples for how to handle cases where data may be null such as publicationAbstract.
        1. Amazon AWS generally charges more when it comes to read-write units as opposed to storage, so be careful about how this code consumes capacity.
        1. To add a new feature:
            1. Add the attribute name in “f.write”.
            1. Add it below with the other fields and their locations in the tree.
            1. Add it in a third location, towards the bottom also under “f.write.”
            1. Each section of code refers to a particular level of the tree structure. Try not to add attributes with other attributes that are at different levels of the tree structure. For example, you wouldn’t add userAssertion at the person level; that belongs at the personArticle level.


## Retrieving CSV data from S3 using Python

1. Prerequisites: if you have not done so already... 
   1. Download the [Visual Studio Code application](https://code.visualstudio.com/download).
   1. We need to connect to S3. Install AWS command line interface as per [these directions](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
   1. Configure AWS CLI. Enter [access key and secret key](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html). 
1. Go to the directory on your machine where you wish to output the data from S3.
1. Create a directory called “s3”
1. Run the following command in Terminal to download data from S3
`aws s3 cp s3://reciter-dynamodb/AnalysisOutput /<yourFolderLocation>/s3 --recursive`
1. Note: everytime before you rerun the command, make sure the file is empty
1. Open S3_AnalysisOutput.py in the Visual Studio application.
1. Set the originalDataPath equal to the location of where the downloaded files are.
1. Set the outputPath equal to the location of where you wish to output the files from this script.
1. Save the file.
1. Execute the Python file:
   1. Option #1: Use the Terminal
      1. In the Terminal, navigate to the location of this Python file.
      1. Execute `python3 S3_AnalysisOutput_upload.py`
      1. The code will output a series of CSV files to the same directory as the Python file
   1. Option #2:
      1. In VisualStudio, right-click on the body of the Python file.
      1. Click on the “Run Python file in Terminal” option
      1. The code will output a series of CSV files to the same directory as the Python file
   1. Notes:
      1. In our experience, for about 1,000 people and 170,000 records, it takes approximately 3-5 minutes to copy these data into local directory. 
      1. To ignore certain tables for export from S3 and comment them out, use three apostrophes before and after the code.


## Retrieving JSON data from the Feature Generator API using a bash script

Here we’re going to use a bash script to retrieve data from ReCiter’s feature generator (or any other) API. This approach affords you greater control and may even return some data that’s not available in S3 or DynamoDB. For example, suppose that your minimumStorageThreshold is set to 4. If you retrieve directly from S3 or DynamoDB, articles with scores below 4 won’t be stored in the system. Additionally, you can take advantage of the Feature Generator API’s ability to return only certain fields.

1. Prerequisites: if you have not done so already... 
   1. You will need to obtain the API key associated with the API you will be querying.
1. Download the `reciterRetrieval.sh` file.
1. Navigate to the directory where this file is stored and change its permissions like so:
1. `chmod +x reciterRetrieval.sh`
1. Open `reciterRetrieval.sh` in a text editor.
1. Update the API key.
1. Update the URL request as you wish. You will want to change anything including: the domain, fields returned, totalStandardizedArticleScore threshold, etc.
1. Save the file.
1. Run `. /reciterRetrieval.sh` from the command line.
1. The script will output a series of JSON files.



## Importing data into a MySQL database and running queries

1. If you haven’t installed MySQL on your system, you will need to do so.
   1. If you haven’t yet installed MySQL, consider MariaDB, which is a fork of MySQL. MariaDB has a [median function](https://mariadb.com/kb/en/library/median/), which may come in handy later.
   1. Follow [these instructions](https://medium.com/@chuanshaoye/install-mariadb-on-mac-7c12502eaec7) to install MariaDB.
1. In your MySQL tool of choice, import the database reciterAnalysis.sql.
1. Each of the data files corresponds to a table in your newly created database. Select each table and import each one, one by one.


## Generating CSV files from JSON output and import into a MySQL database

1. Prerequisites: 
  1. Obtain existing JSON files output from the Feature Generator API. 
  1. Install PyMySQL: `sudo pip install PyMySQL`
  1. Install boto3. Go to command line and enter, `pip install boto3 --user`
1. Import the database reciterAnalysis.sql
1. Go to Terminal enter the following:
```
export DB_HOST=[hostname]
export DB_USERNAME=[username]
export DB_PASSWORD=[password]
export DB_NAME=[database name]
````
1. Open the S3_AnalysisOutput_upload.py file. Update `originalDataPath` with the location of the JSON files. Update `outputPath` with the location of the CSV files to be output.
1. In Terminal, run `python S3_AnalysisOutput_upload.py`
1. This should fully populate a reporting database



### Run queries

1. Now we can run some queries. Here is one to give you the general idea.

```
select distinct personIdentifier, authorFirstName, authorLastName, a.pmid, articleTitle, e.rank, totalAuthorCount, 
case
when e.rank = '1' then 'first'
when e.rank = totalAuthorCount then 'last'
end as firstLast,
publicationTypeCanonical, datePublicationAddedToEntrez
from personArticle
left join personArticleAuthor e on e.personIdentifier = a.personIdentifier and a.pmid = e.pmid
join (select pmid, max(rank) as totalAuthorCount
from personArticleAuthor 
group by pmid) x  on x.pmid = a.pmid
where a.pmid is not null
and userAssertion = 'ACCEPTED'
and datePublicationAddedToEntrez > '2019-05-01' 
and authorTargetAuthor = 1 
```


## Converting ReCiter data from JSON to CSV using Python

These steps will show you how to convert data from JSON retrieved from ReCiter into several CSV files, which can be used in PANDAS, a machine learning tool.

1. Prerequisites: if you have not done so already... 
   1. Download the [Visual Studio Code application](https://code.visualstudio.com/download).
1. Open `ML_Model_Test_upload.py` in the Visual Studio application.
1. Set the originalDataPath equal to the location of where the JSON files are.
1. Set the outputPath equal to the location of where you wish to output the files from this script.
1. Save the file.
1. Execute the Python file:
   1. Option #1: Use the Terminal
      1. In the Terminal, navigate to the location of this file.
      1. Execute `python3 ML_Model_Test_upload.py`
      1. The code will output a series of CSV files to the same directory as the python file
   1. Option #2:
      1. In VisualStudio, right-click on the body of the Python file.
      1. Click on the “Run Python file in Terminal” option
      1. The code will output a series of CSV files to the same directory as the python file



## Using machine learning to optimize scores and thresholds

Here we will learn how to perform analyses on data retrieved from ReCiter so that we can optimize weights and thresholds from application.properties. We will use [pandas](https://pandas.pydata.org/), a sophisticated data analysis library. 

1. Install [Anaconda Python 3.7+](https://www.anaconda.com/distribution/), which includes Jupyter Notebooks.
1. If you haven’t done so already, create a flat file described in the above section, “Converting ReCiter data from JSON to CSV using Python.” This steps prepares data that can be used by the Jupyter Notebook.
1. Open Anaconda.
1. Choose Launch Jupyter Notebooks.
1. If you run the code snippets, you can output any of the following:
   1. Density plot of scores broken down by userAssertion value
   1. Correlation plot of features
   1. Optimized weights for features
1. Here are some sample outputs...

![alt text](https://github.com/wcmc-its/ReCiter-MachineLearning-Analysis/blob/master/BarChartDensityDistribution.png)  

![alt text](https://github.com/wcmc-its/ReCiter-MachineLearning-Analysis/blob/master/DensityBarGraphByUserAssertion.png) 

![alt text](https://github.com/wcmc-its/ReCiter-MachineLearning-Analysis/blob/master/correlationPlotFeatures.png) 
