PredictorFactory
================
Automatic preprocessing of relational data for data mining.

How to run Predictor Factory
============================
1. Go into ```release``` directory
2. Enter in the command line: ```java -jar PF.jar -Dlog4j.configuration=file:config/log4j.properties```

How to edit the source code
===========================
1.	Install JDK 1.8 or newer.
2.	Install Eclipse Luna (R 4.4) for Java Developers or newer.
3.	Start Eclipse.
4.	Import the project from Git (File>Import>Git>Projects from Git): ```https://github.com/janmotl/PredictorFactory.git ```
5.	Add VM argument for logging (Run>Run Configurations...>Arguments): ```-Dlog4j.configuration=file:config/log4j.properties```
6.	Run it (PredictorFactory/src/run/Launcher.java).

Troubleshooting
===============
1. Does Eclipse complain about "TypeUnbound classpath container: JRE System Library [JavaSE-1.8]"? It's likely because you have a different version of JRE. Just change the JRE in the properties (Properties -> Java Build Path -> Libraries Tab -> Select the erroneous system library -> Edit (On the right) -> Select alternate JRE -> Finish)
2. Company firewall can block the access to the database port (3306). Connect over a cellphone to test the hypothesis.
3. Connect with your favourite MySQL/MariaDB tool to the database with read-only access: 
 - Host: relational.fit.cvut.cz
 - Port: 3306
 - Username: guest
 - Password: ```******``` (six asterisks)
