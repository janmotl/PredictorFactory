PredictorFactory
================
Automatic preprocessing of relational data for data mining.

How to Get it Work
==================
1.	Install JDK 1.8 or newer.
2.	Install Eclipse for Java Developers.
3.	Start Eclipse.
4.	Import the project from Git (File>Import>Git>Projects from Git): ```https://github.com/janmotl/PredictorFactory.git ```
5.	Install TestNG plugin (Help>Install New Software…>Work with): ```http://beust.com/eclipse``` 
6.	Run it (PredictorFactory/src/run/Launcher.java).

Troubleshooting
=============
1. Does log4j complain about a missing configuration file? Then add ```PredictorFactory/src/config``` directory to the classpath (Run>Run Configurations>Classpath>User Entries>Advanced>Add Folders).
2. Company firewall can block the access to the database port (3306). Connect over a cellphone to test the hypothesis.
3. Connect with your favourite MySQL/MariaDB tool to the database with read-only access: 
 - Host: relational.fit.cvut.cz
 - Port: 3306
 - Username: guest
 - Password: ```******``` (six asterisks)
