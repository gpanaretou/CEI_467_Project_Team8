## HOW TO BUILD JAR (optional)

To BUILD the .JAR file, we created an IntelliJ project with MAVEN to help with better organising of dependencies.\
Dependencies were added to the pom.xml, where we could easily install them for quick testing of our application.\
Once the .JAVA file was ready, we build the .JAR in intelliJ\
		- BUILD PROJECT\
		- BUILD ARTEFACTS\
		- BUILD Map_Sql.jar

### ALTERNATIVE FOR BUILDING JAR. (easier)

sudo apt install maven\
mvn clean install -> creates a .jar (less space from 70MB to 7kb, does not include dependencies)\
	- this method does not require intelliJ, only maven.


## RUN THE MAPREDUCE JOB
the commands we used for executing the job is as follows:

	bin/hadoop jar *destination_to_the_jar_file* Map_Sql (name of class) *input_file_in_hdfs* *output_dir_in_hdfs* *DISCOUNT* *QUANTITY*
	
	-in our case:
		bin/hadoop jar /mnt/c/Users/gpana/Desktop/Map_Sql.jar Map_Sql /gpanaretou/data/10mb/data/lineitem.tbl 10_mb_disc_0.03 0.03 50
	
	
	- This command calls hadoop to execute a .JAR located a the destination /mnt/c/Users/gpana/Desktop/map_sql.jar
	- Map_Sql is the Class to run from the .JAR file.
	- /gpanaretou/data/10mb/data/lineitem.tbl is the input of the MapReduce application (in HDFS)
	- 10_mb_disc_0.03 is the folder to save the results of the job (in HDFS)
	- 0.03 is the DISCOUNT parameter
	- 50 is the QUANTITY parameter
	
for each execution we simply changed the input and output folders, and the DICSOUNT parameter.
