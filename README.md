PredictorFactory
================
Automatic propositionalization (flattening) of relational data for the purposes of data mining (classification or regression).
See Wiki for more details.

How to run Predictor Factory
============================
1. Download the latest release from GitHub.
2. Unpack the archive.
3. Check Java 8 or newer is installed (```java -version```). Note that the application is not tested for compatibility with OpenJDK.
4. Run PredictorFactory.jar (double click the jar file or enter in the command line: ```java -jar PF.jar```).

How to edit the source code
===========================
1.	Install JDK 1.8 or newer.
2.	Install Eclipse, IDEA, Netbeans or other IDE.
3.  Install Gradle build system.
4.	Import the project from Git: ```https://github.com/janmotl/PredictorFactory.git```.
5.	Run the command line interface from: ```src/run/Launcher.java```.
6.  Or run the graphical user interface from: ```gui/controller/MainApp.java```.

Troubleshooting
===============
1. Company firewall can block the access to the database port. Connect over a cellphone to test the hypothesis.
2. Connect with your favourite database tool to the database to check that the credentials are working.
3. Does your IDE complain during the build process? Check that your IDE is using JDK (not JRE) in the right version (1.8 or higher).
