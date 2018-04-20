PredictorFactory
================
Automatic propositionalization (flattening) of relational data for the purposes of data mining (classification or regression).
See Wiki for more details.

How to run Predictor Factory
============================
1. Download the [latest release](https://github.com/janmotl/PredictorFactory/releases) from GitHub.
2. Unpack the archive.
3. Check Java 8 or newer is installed (```java -version```). Iff you are using OpenJDK, install OpenJFX (```sudo apt-get install openjfx```).
4. Run PredictorFactory.jar (double click the jar file or enter in the command line: ```java -jar PF.jar```).
5. Follow the [manual](https://github.com/janmotl/PredictorFactory/wiki).

How to edit the source code
===========================
1.	Install JDK 1.8 or newer.
2.	Install Eclipse, IDEA, Netbeans or other IDE.
3.  Install Gradle build system.
4.	Import the project from Git: ```https://github.com/janmotl/PredictorFactory.git```.
5.	Run the command line interface from: ```src/run/Launcher.java```.
6.  Or run the graphical user interface from: ```gui/controller/MainApp.java```.
7.  Deploy with Gradle: other>zip. 

##### How to compile ANTLR 4 in Idea 
1. Install ANTLR 4 plugin for IntelliJ IDEA.
2. See [stackoverflow.com] how to setup the directory with the autogenerated code.
3. Set Idea to use javadoc from http://www.antlr.org/api/Java/ (the documentation is not great but better something than nothing).
4. To test a rule, right click on it and select "Test rule bracket".

[stackoverflow.com]:http://stackoverflow.com/questions/23568467/how-to-configure-antlr4-plugin-for-intellij-idea 

How Predictor Factory works
===========================
1.  Collect metadata about the database (list of tables, columns and foreign key constraints).
2.  Create a "base table" from the "target table". Base table is a (subsample) of the target table with just the essential attributes (id, target, and optionally a timestamp), which gets propagated into all other tables.
3.  Propagate base table into all tables in the database with joins, as defined by the foreign key constraints.
4.  Calculate predictors on the propagated tables.
5.  Join the calculated predictors into the mainsample table.

Troubleshooting
===============
1. Company firewall can block the access to the database port. Connect over a cellphone to test the hypothesis.
2. Connect with your favourite database tool to the database to check that the credentials are working.
3. Does your IDE complain during the build process? Check that your IDE is using JDK (not JRE) in the right version (1.8 or higher).
