Jack Menjivar - s3602756

PART 1: Small Change to Creation of Table, Primary Key added to End of int ID
PART 2: MongoDB Script changed to facilitate more complex document structure.
Script is compiled with:
       javac -cp json-simple-1.1.1.jar App.java
and is run with:
       java json-simple.1.1.1.jar App
PART 3: 
DBLoad is the same as Assignment 1 except that the number of records in the page is contained within the pagesize (i.e. before it was 4096 + 8 now it is just 4096 pages)
Hashload is compiled normally and is run with java hashload pagesize
Hashquery is compiled normally and is run with java hashquery pagesize query or java hashquery query pagesize
