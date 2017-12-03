
Files
0. this README
1. wh               - the directory containing the src folder 
2. wh/src/main/java - the source folder
3. pom.xml                 - a maven pom for compiling
4. wh/.project      - an Eclipse maven project file
5. Parser.java             - the main application
6. LoadToDB.java           - a java class to load the data into the db
7. create-log.sql          - the sql to create the two tables
8. test-query.sql          - the unit test queries
9. tar.tar                 - a tar of all these files excluding tar.tar

About
LoadToDB cleans the schema1.log table and loads the data from the access.log file.
Parser does as required by instructions. See Code Choices for explanation of code.

To Compile
Use javac or 
1. cd wh
2. mvn clean package

To Run
1. cd wh
2. java -jar target/Parser
3 java -jar target/Parser-1.0.jar  --accesslog=your-access-log  --startDate="2017-01-01.15:00:00 " --duration=hourly --threshold=200
example:
java -jar target/Parser-1.0.jar  --accesslog="/Users/mm/Downloads/wh/Java_MySQL_Test/access.log"  --startDate="2017-01-01.15:00:00 " --duration=hourly --threshold=200


To Load the Data
java -cp target/Parser-1.0.jar com.ef.LoadToDB  access.log  
or
java -cp target/Parser-1.0.jar com.ef.LoadToDB  access.log --clean

The second cleans the database first, deleting all records in the log table

/Users/mm/Downloads/wh/Java_MySQL_Test/access.log 

Queries for Unit Testing

SELECT ip ,count(*) requestCount from schema1.log where request_time between '2017-01-01.15:00:00' and '2017-01-01.15:59:59' group by ip order by requestCount desc;

SELECT ip ,count(*) requestCount from schema1.log where request_time between '2017-01-01.00:00:00 ' and '2017-01-01.23:59:59' group by ip order by requestCount desc;

# using bash for unit testing
# this shows that 192.168.129.191 has 747 records for 2017-01-01.
cat access.log |egrep '192.168.129.191'|sort|grep '2017-01-01'|cut -d '|' -f1 > 192.168.129.191.txt



SQL for log table and blocked_ip

CREATE TABLE schema1.log
(
  id bigint NOT NULL AUTO_INCREMENT,
  request_time timestamp,
  ip text,
  request_type text,
  status int,
  browser text,
  CONSTRAINT description_pkey PRIMARY KEY (id)
);

# create second table for output
CREATE TABLE schema1.blocked_ip
(
  id bigint NOT NULL AUTO_INCREMENT,
  ip text,
  comments text,
  CONSTRAINT description_pkey PRIMARY KEY (id)
);

Code Choices
I chose to use straight jdbc for this project. This is more of a utility project and jdbc is simple and fast. Another good alternative would be Spring JDBC Template. I would not use JPA or any ORM.
I chose prepared statement as it is syntactictally cleaner and in general safer than building a sql string for execution. It is common practice to use prepared statement in user interfaces to avoid sql injection. 

Spring JDBC Template offers several advantages:
1. dependency injection of values, like database url, schema etc is easily managed with application properties file and appropriate @Value annotation.
2. Mapper and Template classes make coding very simple
3. Container management means less code for object creation etc.

Were I to use  Spring JDBC Template, I would create:
1. create a POJO for the log events, call it LogEvent 
2. create a mapper class which implements org.springframework.jdbc.core.RowMapper<LogEvent>
3. create a DAO interface with CRUD methods 
4. create a DAO class which implements the interface, call it LogEventTemplate. 
5. use @Autowire, @Value and other annotations for configuration, autowiring and dependency injection. For example in the main class I might choose to have a container managed variable like this:

@Autowired 
private EventLogTemplate eventLogTemplate;

6. put all DB related classes in separate library which can be compiled completely separately. 


