/*LBE
Code for Stream and Task Workshop - Student Version
Interactive learning with exercises and questions

INSTRUCTIONS FOR STUDENTS:
==========================

1. PROGRESSIVE DIFFICULTY:
   - Section 1-2: BEGINNER (Basic setup and data ingestion)
   - Section 3-4: INTERMEDIATE (JSON parsing and streams)
   - Section 5-6: ADVANCED (Task orchestration and monitoring)
   - Section 7-8: EXPERT (Complex orchestration and optimization)
   - Section 9-10: MASTER LVL 100 BOSS (Data quality and PII protection)

2. LEARNING APPROACH:
   - Start with Section 1 and work sequentially
   - Each section builds on the previous one
   - Complete all YOUR CODE HERE sections
   - Answer all questions for deeper understanding
   - Use hints when needed - they're there to help!

3. HINTS SYSTEM:
   - All hints and solutions are in a separate file: snowflake_streams_tasks_hints.sql
   - Try to solve exercises first without looking at hints
   - When stuck, check the hints file for the corresponding exercise number
   - Hints are numbered to match exercise numbers (e.g., HINT 1.1, HINT 1.2, etc.)

4. ASSESSMENT:
   - Complete all sections for basic understanding
   - Master sections 1-6 for intermediate level
   - Complete sections 7-10 for advanced level
   - Bonus challenges for expert level

Good luck with your Snowflake learning !
*/

-- ===========================================
-- SECTION 1: SETUP AND PREPARATION (BEGINNER LEVEL)
-- ===========================================

-- Exercise 1.1: Create the necessary role and permissions
-- DIFFICULTY: BEGINNER
-- TODO: Complete the role creation and grant statements below
-- HINTS: Check hints file for HINT 1.1 through HINT 1.5

use role ACCOUNTADMIN;
set myname = current_user();

-- YOUR CODE HERE - Create role Data_ENG
-- YOUR CODE HERE - Grant role to current user
-- YOUR CODE HERE - Grant create database permission
-- YOUR CODE HERE - Grant task execution permissions
-- YOUR CODE HERE - Grant imported privileges on SNOWFLAKE database

-- Exercise 1.2: Create warehouse and database
-- DIFFICULTY: BEGINNER
-- TODO: Create a warehouse and database for this lab
-- HINTS: Check hints file for HINT 1.6 through HINT 1.9

-- YOUR CODE HERE - Create warehouse Orchestration_WH (XSMALL, auto-suspend 5 min)
-- YOUR CODE HERE - Grant warehouse privileges to Data_ENG role
-- YOUR CODE HERE - Create database Credit_card
-- YOUR CODE HERE - Grant database privileges to Data_ENG role

-- Switch to the new role and database
use role Data_ENG;
use database Credit_card;
use schema PUBLIC;
use warehouse Orchestration_WH;

-- ===========================================
-- SECTION 2: DATA INGESTION SETUP (BEGINNER LEVEL)
-- ===========================================

-- Exercise 2.1: Create staging infrastructure
-- DIFFICULTY: BEGINNER
-- TODO: Create the necessary objects for data ingestion
-- HINTS: Check hints file for HINT 2.1 through HINT 2.2

-- YOUR CODE HERE - Create internal stage CC_STAGE with JSON file format
-- YOUR CODE HERE - Create staging table CC_TRANS_STAGING with VARIANT column

-- Question 2.1: Why do we use a VARIANT column for JSON data?
-- Answer: _________________________________________________

-- Exercise 2.2: Create the data generation stored procedure
-- TODO: This is provided for you - study the Java code to understand how it works

create or replace procedure SIMULATE_KAFKA_STREAM(mystage STRING,prefix STRING,numlines INTEGER)
  RETURNS STRING
  LANGUAGE JAVA
  PACKAGES = ('com.snowflake:snowpark:latest')
  HANDLER = 'StreamDemo.run'
  AS
  $$
    import com.snowflake.snowpark_java.Session;
    import java.io.*;
    import java.util.HashMap;
    public class StreamDemo {
      public String run(Session session, String mystage,String prefix,int numlines) {
        SampleData SD=new SampleData();
        BufferedWriter bw = null;
        File f=null;
        try {
            f = File.createTempFile(prefix, ".json");
            FileWriter fw = new FileWriter(f);
	        bw = new BufferedWriter(fw);
            boolean first=true;
            bw.write("[");
            for(int i=1;i<=numlines;i++){
                if (first) first = false;
                else {bw.write(",");bw.newLine();}
                bw.write(SD.getDataLine(i));
            }
            bw.write("]");
            bw.close();
            return session.file().put(f.getAbsolutePath(),mystage,options)[0].getStatus();
        }
        catch (Exception ex){
            return ex.getMessage();
        }
        finally {
            try{
	            if(bw!=null) bw.close();
                if(f!=null && f.exists()) f.delete();
	        }
            catch(Exception ex){
	            return ("Error in closing:  "+ex);
	        }
        }
      }

      private static final HashMap<String,String> options = new HashMap<String, String>() {
        { put("AUTO_COMPRESS", "TRUE"); }
      };

      public static class SampleData {
      private static final java.util.Random R=new java.util.Random();
      private static final java.text.NumberFormat NF_AMT = java.text.NumberFormat.getInstance();
      String[] transactionType={"PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","REFUND"};
      String[] approved={"true","true","true","true","true","true","true","true","true","true","false"};
      static {
        NF_AMT.setMinimumFractionDigits(2);
        NF_AMT.setMaximumFractionDigits(2);
        NF_AMT.setGroupingUsed(false);
      }

      private static int randomQty(int low, int high){
        return R.nextInt(high-low) + low;
      }

      private static double randomAmount(int low, int high){
        return R.nextDouble()*(high-low) + low;
      }

      private String getDataLine(int rownum){
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("\"element\":"+rownum+",")
            .append("\"object\":\"basic-card\",")
            .append("\"transaction\":{")
            .append("\"id\":"+(1000000000 + R.nextInt(900000000))+",")
            .append("\"type\":"+"\""+transactionType[R.nextInt(transactionType.length)]+"\",")
            .append("\"amount\":"+NF_AMT.format(randomAmount(1,5000)) +",")
            .append("\"currency\":"+"\"USD\",")
            .append("\"timestamp\":\""+java.time.Instant.now()+"\",")
            .append("\"approved\":"+approved[R.nextInt(approved.length)]+"")
            .append("},")
            .append("\"card\":{")
                .append("\"number\":"+ java.lang.Math.abs(R.nextLong()) +"")
            .append("},")
            .append("\"merchant\":{")
            .append("\"id\":"+(100000000 + R.nextInt(90000000))+"")
            .append("}")
            .append("}");
        return sb.toString();
      }
    }
}
$$;

-- Question 2.2: What does this stored procedure simulate?
-- Answer: _________________________________________________

-- Exercise 2.3: Test data generation
-- DIFFICULTY: BEGINNER
-- TODO: Call the stored procedure and verify the results
-- HINTS: Check hints file for HINT 2.3 through HINT 2.6

-- YOUR CODE HERE - Call SIMULATE_KAFKA_STREAM with appropriate parameters
-- YOUR CODE HERE - List files in the stage to verify creation
-- YOUR CODE HERE - Copy data from stage to staging table
-- YOUR CODE HERE - Check row count in staging table

-- ===========================================
-- SECTION 3: JSON DATA EXPLORATION (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 3.1: Explore JSON structure
-- DIFFICULTY: INTERMEDIATE
-- TODO: Write queries to explore the JSON data structure

-- YOUR CODE HERE - Select card numbers from the JSON data
-- YOUR CODE HERE - Parse and display transaction details (id, amount, currency, approved, type, timestamp)
-- YOUR CODE HERE - Filter transactions with amount < 600

-- Question 3.1: What is the advantage of using VARIANT columns for JSON data?
-- Answer: _________________________________________________

-- Exercise 3.2: Create a normalized view
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a view that flattens the JSON structure into columns

-- YOUR CODE HERE - Create view CC_TRANS_STAGING_VIEW with proper column mapping
-- YOUR CODE HERE - Enable change tracking on table and view
-- YOUR CODE HERE - Test the view with sample queries

-- Question 3.2: Why do we need to enable change tracking?
-- Answer: _________________________________________________

-- ===========================================
-- SECTION 4: STREAMS AND CHANGE DATA CAPTURE (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 4.1: Create and test streams
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a stream on the view and explore its behavior

-- YOUR CODE HERE - Create stream CC_TRANS_STAGING_VIEW_STREAM on the view
-- YOUR CODE HERE - Check initial stream content
-- YOUR CODE HERE - Count records in the stream

-- Question 4.1: What does SHOW_INITIAL_ROWS=true do in stream creation?
-- Answer: _________________________________________________

-- Exercise 4.2: Create analytical table
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a normalized table for analytics

-- YOUR CODE HERE - Create table CC_TRANS_ALL with proper schema
-- YOUR CODE HERE - Insert data from stream into analytical table
-- YOUR CODE HERE - Verify data in analytical table

-- Question 4.2: What is the difference between the staging table and analytical table?
-- Answer: _________________________________________________

-- ===========================================
-- SECTION 5: TASK ORCHESTRATION (ADVANCED LEVEL)
-- ===========================================

-- Exercise 5.1: Create your first task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that generates data automatically

-- YOUR CODE HERE - Create task GENERATE_TASK with 1-minute schedule
-- YOUR CODE HERE - Describe the task to see its definition
-- YOUR CODE HERE - Execute the task manually
-- YOUR CODE HERE - Resume the task to run on schedule

-- Question 5.1: What are the benefits of using tasks vs manual execution?
-- Answer: _________________________________________________

-- Exercise 5.2: Create data processing task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that processes files from stage to staging table

-- YOUR CODE HERE - Create task PROCESS_FILES_TASK with 3-minute schedule
-- YOUR CODE HERE - Execute task manually and verify results
-- YOUR CODE HERE - Resume the task

-- Exercise 5.3: Create data refinement task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that processes stream data into analytical table

-- YOUR CODE HERE - Create task REFINE_TASK with stream condition
-- YOUR CODE HERE - Execute task manually and verify results
-- YOUR CODE HERE - Resume the task

-- Question 5.2: What does SYSTEM$STREAM_HAS_DATA() do?
-- Answer: _________________________________________________

-- ===========================================
-- SECTION 6: MONITORING AND REPORTING (ADVANCED LEVEL)
-- ===========================================

-- Exercise 6.1: Monitor task execution
-- DIFFICULTY: ADVANCED
-- TODO: Create monitoring queries

-- YOUR CODE HERE - Query load history from INFORMATION_SCHEMA
-- YOUR CODE HERE - Query load history from ACCOUNT_USAGE
-- YOUR CODE HERE - Check task execution history

-- Question 6.1: What is the difference between INFORMATION_SCHEMA and ACCOUNT_USAGE?
-- Answer: _________________________________________________

-- Exercise 6.2: Analyze data flow
-- DIFFICULTY: ADVANCED
-- TODO: Create queries to understand data flow

-- YOUR CODE HERE - Count records in each table (staging, view, stream, analytical)
-- YOUR CODE HERE - Find the latest transaction timestamp
-- YOUR CODE HERE - Analyze transaction patterns (approved vs rejected)

-- ===========================================
-- SECTION 7: ADVANCED ORCHESTRATION (EXPERT LEVEL)
-- ===========================================

-- Exercise 7.1: Create task dependencies
-- DIFFICULTY: EXPERT
-- TODO: Create a sequential task pipeline

-- YOUR CODE HERE - Create tasks for pipeline 2
-- YOUR CODE HERE - Set up task dependencies using ALTER TASK ... ADD AFTER
-- YOUR CODE HERE - Create a root task that triggers the pipeline

-- Question 7.1: How do task dependencies work in Snowflake?
-- Answer: _________________________________________________

-- Exercise 7.2: Parallel processing
-- DIFFICULTY: EXPERT
-- TODO: Create tasks that can run in parallel

-- YOUR CODE HERE - Create a wait task for parallel processing
-- YOUR CODE HERE - Set up parallel task execution
-- YOUR CODE HERE - Monitor task dependencies

-- ===========================================
-- SECTION 8: CLEANUP AND BEST PRACTICES (EXPERT LEVEL)
-- ===========================================

-- Exercise 8.1: Task management
-- DIFFICULTY: EXPERT
-- TODO: Properly manage task lifecycle

-- YOUR CODE HERE - Suspend all running tasks
-- YOUR CODE HERE - Show all tasks and their states
-- YOUR CODE HERE - Create a cleanup script

-- Question 8.1: Why is it important to suspend tasks when not needed?
-- Answer: _________________________________________________

-- Exercise 8.2: Performance analysis
-- DIFFICULTY: EXPERT
-- TODO: Analyze the performance of your pipeline

-- YOUR CODE HERE - Calculate total processing time
-- YOUR CODE HERE - Analyze data volume processed
-- YOUR CODE HERE - Identify potential bottlenecks

-- ===========================================
-- SECTION 9: COMPREHENSIVE DATA QUALITY CHECKS (MASTER LEVEL)
-- ===========================================

-- Exercise 9.1: Basic data quality validation
-- DIFFICULTY: INTERMEDIATE
-- TODO: Implement comprehensive data quality checks

-- YOUR CODE HERE - Check for duplicate transactions
-- YOUR CODE HERE - Validate data types and ranges
-- YOUR CODE HERE - Check for missing values
-- YOUR CODE HERE - Validate card number format
-- YOUR CODE HERE - Check for data anomalies

-- Exercise 9.2: Advanced data quality metrics
-- DIFFICULTY: ADVANCED
-- TODO: Create comprehensive data quality dashboard

-- YOUR CODE HERE - Create data quality metrics table
-- YOUR CODE HERE - Calculate completeness metrics
-- YOUR CODE HERE - Calculate validity metrics
-- YOUR CODE HERE - Calculate consistency metrics
-- YOUR CODE HERE - Create data quality dashboard

-- Exercise 9.3: Data quality monitoring and alerting
-- DIFFICULTY: EXPERT
-- TODO: Implement automated data quality monitoring

-- YOUR CODE HERE - Create data quality monitoring procedure
-- YOUR CODE HERE - Create automated quality check task
-- YOUR CODE HERE - Implement quality alerting system
-- YOUR CODE HERE - Create quality trend analysis

-- ===========================================
-- SECTION 10: PII PROTECTION AND DATA MASKING (MASTER LEVEL)
-- ===========================================

-- Exercise 10.1: Identify PII in credit card data
-- DIFFICULTY: INTERMEDIATE
-- TODO: Identify and categorize PII fields

-- YOUR CODE HERE - Identify PII fields in the data
-- YOUR CODE HERE - Create PII classification table
-- YOUR CODE HERE - Classify all fields by sensitivity
-- YOUR CODE HERE - Determine masking requirements

-- Exercise 10.2: Implement data masking for PII
-- DIFFICULTY: ADVANCED
-- TODO: Create masked views for different user roles

-- YOUR CODE HERE - Create masked view for analysts
-- YOUR CODE HERE - Create masked view for auditors
-- YOUR CODE HERE - Implement role-based access control
-- YOUR CODE HERE - Test masking effectiveness

-- Exercise 10.3: Advanced PII protection strategies
-- DIFFICULTY: EXPERT
-- TODO: Implement advanced PII protection mechanisms

-- YOUR CODE HERE - Create dynamic masking policy
-- YOUR CODE HERE - Apply masking to sensitive columns
-- YOUR CODE HERE - Implement data retention policy
-- YOUR CODE HERE - Create data anonymization procedure
-- YOUR CODE HERE - Test PII protection mechanisms

-- Question 10.1: What are the key principles of PII protection in data systems?
-- Answer: _________________________________________________

-- Question 10.2: How would you implement GDPR compliance for this credit card data?
-- Answer: _________________________________________________

-- Question 10.3: What are the trade-offs between data utility and privacy protection?
-- Answer: _________________________________________________

-- ===========================================
-- BONUS CHALLENGES
-- ===========================================

-- Challenge 1: Dagster integration with Dagster free trial and the token

-- Challenge 2: Implement data partitioning
-- TODO: Add partitioning to improve query performance

-- Challenge 3: Create a data lineage tracking system
-- TODO: Track data flow from source to analytics

-- Challenge 4: Implement real-time alerting
-- TODO: Create alerts for data anomalies

-- Challenge 5: Optimize for cost
-- TODO: Implement cost optimization strategies

-- ===========================================
-- LAB COMPLETION CHECKLIST
-- ===========================================

-- □ Created all necessary roles and permissions
-- □ Set up warehouse and database
-- □ Created staging infrastructure
-- □ Implemented data generation procedure
-- □ Created and tested streams
-- □ Built analytical tables
-- □ Created and configured tasks
-- □ Implemented task orchestration
-- □ Set up monitoring and reporting
-- □ Understood data flow and dependencies
-- □ Implemented proper cleanup procedures
-- □ Completed all exercises and questions
-- □ Attempted bonus challenges

-- Congratulations! You have completed the Snowflake Streams and Tasks workshop!
-- You now understand how to build real-time data pipelines using Snowflake's
-- streaming and orchestration capabilities.
