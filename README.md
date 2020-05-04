# Movies-ETL

## Challenge Overview 
Requested to write a Python script that performs all three ETL steps on the Wikipedia and Kaggle data. The requirements are to create an automated ETL pipeline that extracts data from multiple sources, cleans and transform the data, and loads data into SQL database. 

## Challenge Summary 

### Deliverables:
 - Movies Imported Final Row Count: 6,051
  - Ratings Imported Row Count: 26,024,289
  
Function Created: ETL_MovieData()

### Assumption Analysis Documentation: 
Additional problems that may arise with ETL Function with new data: 
 - Loading Files: (FileNotFoundError) Adding Try-Except blocks to alert the user if data files are not found, and to inform user to place files within same folder level as python function file. 
 - Loading CSVs: (DtypeWarning) Within some metadata of some CSVs, a datatype error would be thrown that would stop the extracting process because of mixed datatypes within a specified column. Adding a low_memory=False condition would ensure the file would be read into the data frame without throwing the error. 
 - Empty Data: (EmptyDataError) When attempting to load and extract data, and if there is no data within file(s) due to corrupted download or erased information, an exception should be raised an notify the user to verify data source. 
 - Missing Column Data: When beginning the transformation process on extracted files, if a column is missing from the data frame and it is specified within a method or function, it would throw a KeyError. An exception would need to be made or further investigation on the incoming data to determine if another column should be used or created in its stead. 
 - Introduction of new columns: If future incoming data has additional columns of data, code would error when performing changes on table. Would need to be determined if additional columns are to be kept, or have an exception error to exclude any additional column data than previously established. For example, if an additional column for an alternate title was introduced, then the current cleaning function would be affected for the keys would need to be updated to include the new column. 
 - New Row Data Formats: If incoming data were to be entered in a different format that is not previously included in transformation functions and statements when data cleaning, the cell data will be saved to the table and would lead to errors in database. This would most likely occur for numerical values, strings, and datetime formats. 
 - Data Duplicates: Only certain duplicate row entries are accounted for in current function. Would need to expand to confirm other methods of duplicate entries and either drop information or include them in data set in a certain manner. 
 - NaN Values: If change in data source allowed for extra “NaN” values, would affect various parsing and transformations. If for instance movie data was missing multiple titles, there would be row data that would belong to unknown movies that would appear in the database when imported. 
 - Value Errors: If a string value was placed within an integer value from the extraction process, functions would fail and any performed calculations would give an error. If data such as “10 PM” was entered within datetype, the string would interrupt the function or change run upon it and throw an error. 
 - Missing/Altered Merge Columns: If incoming extracted data changes the name or format of main columns used when merging datasets together, would create a large error that would stop the process, if not corrupt the process of the rows being merged.  
