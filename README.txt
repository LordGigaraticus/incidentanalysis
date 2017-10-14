Name: Dakotah Rickert
Version: 1.0
Date: 7/27/17
Project Name: Incident Analysis

Purpose:
//-------------------------------------------------------------------------------------------------
The purpose of this project is to take two CSV files containing data related to the number of
service requests and traffic accidents within the Denver area and loading them into a queryable
database in order to investigate meaningful trends and patterns within the data.
//-------------------------------------------------------------------------------------------------

Solution:
//-------------------------------------------------------------------------------------------------
In order to accomplish this, I chose to load the data into two separate postgres tables within
a remote server I manage within my home. I loaded the data using scalikeJDBC.

After loading the data, I used Spark SQL to aggregate the data into meaningful data frames
that presented the data in various forms in order to determine useful trends like which
neighborhoods have the most traffic accidents or what day of the year has the most
service requests.

The project is built using Apache Maven as the framework with Java and Scala as the main languages.
//-------------------------------------------------------------------------------------------------

Build:
//-------------------------------------------------------------------------------------------------
Provided Maven is installed on the machine, the build command below is ran from the project directory:

mvn clean install

If Maven is not installed, I have prebuilt the project and provided the jar within the target folder.
The jar's path is printed below:

project/target/project-1.0-SNAPSHOT.jar
//-------------------------------------------------------------------------------------------------

Run:
//-------------------------------------------------------------------------------------------------
Provided Java is installed, the run command is printed below:

java -cp project-1.0-SNAPSHOT.jar IncidentAnalysis
//-------------------------------------------------------------------------------------------------

Notes:
//-------------------------------------------------------------------------------------------------
The database should be queryable as long as the project has been run at least once. This is because
one of the functions within the project actually builds both tables, so in order to guarantee success,
please run the project first. The credentials for accessing the database are located within the
project.properties file, but I have also printed them below for your ease of access.

URL: jdbc:postgresql://107.2.146.56:5432/project
Host: 107.2.146.56
Port: 5432
Driver: org.postgres.Driver
Database: project
Username: project
Password: tcejorp123

Unfortunately, since I manage this database out of my own home and I do not have a fixed IP address,
there is a very small possibility that the host could change if my internet ever resets and a user
would be unable to access the database. Please email me if this occurs at
rickertdb@gmail.com and I will provide you with the new host address.
//-------------------------------------------------------------------------------------------------