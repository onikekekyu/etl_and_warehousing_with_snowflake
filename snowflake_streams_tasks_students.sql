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

-- Create role Data_ENG
create role if not exists Data_ENG;
-- Grant role to current user
grant role Data_ENG to user identifier($myname);
-- Grant create database permission
grant create database on account to role Data_ENG;
-- Grant task execution permissions
grant execute task on account to role Data_ENG;
-- Grant imported privileges on SNOWFLAKE database
grant imported privileges on database SNOWFLAKE to role Data_ENG;

-- Exercise 1.2: Create warehouse and database
-- DIFFICULTY: BEGINNER
-- TODO: Create a warehouse and database for this lab
-- HINTS: Check hints file for HINT 1.6 through HINT 1.9

-- Create warehouse Orchestration_WH (XSMALL, auto-suspend 5 min)
create warehouse if not exists Orchestration_WH with warehouse_size = 'XSMALL' auto_suspend = 300;
-- Grant warehouse privileges to Data_ENG role
grant usage on warehouse Orchestration_WH to role Data_ENG;
-- Create database Credit_card
create database if not exists Credit_card;
-- Grant database privileges to Data_ENG role
grant all privileges on database Credit_card to role Data_ENG;

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

-- Create internal stage CC_STAGE with JSON file format
create or replace stage CC_STAGE
file_format = (type = JSON);

-- Create staging table CC_TRANS_STAGING with VARIANT column
create or replace table CC_TRANS_STAGING (
    json_data VARIANT
);

-- Question 2.1: Why do we use a VARIANT column for JSON data?
-- Answer: VARIANT columns allow storing semi-structured data like JSON without needing to define a schema upfront. They provide flexibility to handle nested objects, arrays, and varying structures while maintaining query performance through automatic indexing and optimization.

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
-- Answer: This stored procedure simulates a Kafka stream by generating synthetic credit card transaction data in JSON format. It creates realistic transaction records with random amounts, card numbers, merchant IDs, timestamps, and approval statuses to mimic real-time streaming data ingestion.

-- Exercise 2.3: Test data generation
-- DIFFICULTY: BEGINNER
-- TODO: Call the stored procedure and verify the results
-- HINTS: Check hints file for HINT 2.3 through HINT 2.6

-- Call SIMULATE_KAFKA_STREAM with appropriate parameters
call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'cc_trans', 100);

-- List files in the stage to verify creation
list @CC_STAGE;

-- Copy data from stage to staging table
copy into CC_TRANS_STAGING from @CC_STAGE
file_format = (type = JSON);

-- Check row count in staging table
select count(*) from CC_TRANS_STAGING;

-- ===========================================
-- SECTION 3: JSON DATA EXPLORATION (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 3.1: Explore JSON structure
-- DIFFICULTY: INTERMEDIATE
-- TODO: Write queries to explore the JSON data structure

-- Select card numbers from the JSON data
select json_data:card.number as card_number
from CC_TRANS_STAGING
limit 10;

-- Parse and display transaction details (id, amount, currency, approved, type, timestamp)
select 
    json_data:transaction.id::bigint as transaction_id,
    json_data:transaction.amount::number(10,2) as amount,
    json_data:transaction.currency::string as currency,
    json_data:transaction.approved::boolean as approved,
    json_data:transaction.type::string as transaction_type,
    json_data:transaction.timestamp::timestamp as transaction_timestamp
from CC_TRANS_STAGING
limit 10;

-- Filter transactions with amount < 600
select *
from CC_TRANS_STAGING
where json_data:transaction.amount::number < 600
limit 10;

-- Question 3.1: What is the advantage of using VARIANT columns for JSON data?
-- Answer: VARIANT columns provide schema flexibility, automatic compression, native JSON path notation for querying, type inference, and optimized storage. They eliminate the need for ETL transformations and allow querying nested data directly without complex parsing logic.

-- Exercise 3.2: Create a normalized view
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a view that flattens the JSON structure into columns

-- Create view CC_TRANS_STAGING_VIEW with proper column mapping
create or replace view CC_TRANS_STAGING_VIEW as
select 
    json_data:element::int as element,
    json_data:object::string as object_type,
    json_data:transaction.id::bigint as transaction_id,
    json_data:transaction.type::string as transaction_type,
    json_data:transaction.amount::number(10,2) as amount,
    json_data:transaction.currency::string as currency,
    json_data:transaction.timestamp::timestamp as transaction_timestamp,
    json_data:transaction.approved::boolean as approved,
    json_data:card.number::bigint as card_number,
    json_data:merchant.id::bigint as merchant_id
from CC_TRANS_STAGING;

-- Enable change tracking on table and view
alter table CC_TRANS_STAGING set change_tracking = true;
alter view CC_TRANS_STAGING_VIEW set change_tracking = true;

-- Test the view with sample queries
select * from CC_TRANS_STAGING_VIEW limit 5;
select count(*) from CC_TRANS_STAGING_VIEW;
select avg(amount) as avg_amount from CC_TRANS_STAGING_VIEW where approved = true;

-- Question 3.2: Why do we need to enable change tracking?
-- Answer: Change tracking is required to create streams on tables and views. It enables Snowflake to track data changes (inserts, updates, deletes) and capture them in streams for real-time data processing and change data capture (CDC) scenarios.

-- ===========================================
-- SECTION 4: STREAMS AND CHANGE DATA CAPTURE (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 4.1: Create and test streams
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a stream on the view and explore its behavior

-- Create stream CC_TRANS_STAGING_VIEW_STREAM on the view
create or replace stream CC_TRANS_STAGING_VIEW_STREAM on view CC_TRANS_STAGING_VIEW
show_initial_rows = true;

-- Check initial stream content
select * from CC_TRANS_STAGING_VIEW_STREAM limit 10;

-- Count records in the stream
select count(*) from CC_TRANS_STAGING_VIEW_STREAM;

-- Question 4.1: What does SHOW_INITIAL_ROWS=true do in stream creation?
-- Answer: SHOW_INITIAL_ROWS=true makes the stream return all existing rows in the table/view as INSERT operations when the stream is first created. Without this option, the stream would only capture changes that occur after stream creation.

-- Exercise 4.2: Create analytical table
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a normalized table for analytics

-- Create table CC_TRANS_ALL with proper schema
create or replace table CC_TRANS_ALL (
    element int,
    object_type string,
    transaction_id bigint,
    transaction_type string,
    amount number(10,2),
    currency string,
    transaction_timestamp timestamp,
    approved boolean,
    card_number bigint,
    merchant_id bigint,
    loaded_at timestamp default current_timestamp()
);

-- Insert data from stream into analytical table
insert into CC_TRANS_ALL 
(element, object_type, transaction_id, transaction_type, amount, currency, 
 transaction_timestamp, approved, card_number, merchant_id)
select 
    element, object_type, transaction_id, transaction_type, amount, currency,
    transaction_timestamp, approved, card_number, merchant_id
from CC_TRANS_STAGING_VIEW_STREAM
where metadata$action = 'INSERT';

-- Verify data in analytical table
select count(*) from CC_TRANS_ALL;
select * from CC_TRANS_ALL limit 10;

-- Question 4.2: What is the difference between the staging table and analytical table?
-- Answer: The staging table stores raw JSON data in VARIANT columns for initial ingestion and flexibility. The analytical table has a normalized schema with typed columns optimized for analytics, reporting, and downstream processing with better query performance.

-- ===========================================
-- SECTION 5: TASK ORCHESTRATION (ADVANCED LEVEL)
-- ===========================================

-- Exercise 5.1: Create your first task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that generates data automatically

-- Create task GENERATE_TASK with 1-minute schedule
create or replace task GENERATE_TASK
    warehouse = Orchestration_WH
    schedule = '1 minute'
as
    call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'cc_trans', 50);

-- Describe the task to see its definition
describe task GENERATE_TASK;

-- Execute the task manually
execute task GENERATE_TASK;

-- Resume the task to run on schedule
alter task GENERATE_TASK resume;

-- Question 5.1: What are the benefits of using tasks vs manual execution?
-- Answer: Tasks provide automation, consistent scheduling, dependency management, error handling, monitoring capabilities, and scalability. They eliminate manual intervention, reduce human error, ensure reliable data pipeline execution, and enable complex orchestration workflows.

-- Exercise 5.2: Create data processing task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that processes files from stage to staging table

-- Create task PROCESS_FILES_TASK with 3-minute schedule
create or replace task PROCESS_FILES_TASK
    warehouse = Orchestration_WH
    schedule = '3 minute'
as
    copy into CC_TRANS_STAGING from @CC_STAGE
    file_format = (type = JSON)
    force = true;

-- Execute task manually and verify results
execute task PROCESS_FILES_TASK;
select count(*) from CC_TRANS_STAGING;

-- Resume the task
alter task PROCESS_FILES_TASK resume;

-- Exercise 5.3: Create data refinement task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that processes stream data into analytical table

-- Create task REFINE_TASK with stream condition
create or replace task REFINE_TASK
    warehouse = Orchestration_WH
    schedule = '2 minute'
    when system$stream_has_data('CC_TRANS_STAGING_VIEW_STREAM')
as
    insert into CC_TRANS_ALL 
    (element, object_type, transaction_id, transaction_type, amount, currency, 
     transaction_timestamp, approved, card_number, merchant_id)
    select 
        element, object_type, transaction_id, transaction_type, amount, currency,
        transaction_timestamp, approved, card_number, merchant_id
    from CC_TRANS_STAGING_VIEW_STREAM
    where metadata$action = 'INSERT';

-- Execute task manually and verify results
execute task REFINE_TASK;
select count(*) from CC_TRANS_ALL;

-- Resume the task
alter task REFINE_TASK resume;

-- Question 5.2: What does SYSTEM$STREAM_HAS_DATA() do?
-- Answer: SYSTEM$STREAM_HAS_DATA() is a system function that returns TRUE if the specified stream contains data (has captured changes), and FALSE otherwise. It's used in task WHEN clauses to conditionally execute tasks only when there's data to process, improving efficiency and reducing unnecessary executions.

-- ===========================================
-- SECTION 6: MONITORING AND REPORTING (ADVANCED LEVEL)
-- ===========================================

-- Exercise 6.1: Monitor task execution
-- DIFFICULTY: ADVANCED
-- TODO: Create monitoring queries

-- Query load history from INFORMATION_SCHEMA
select * from information_schema.load_history
where table_name = 'CC_TRANS_STAGING'
order by last_load_time desc
limit 10;

-- Query load history from ACCOUNT_USAGE
select * from snowflake.account_usage.load_history
where table_name = 'CC_TRANS_STAGING'
order by last_load_time desc
limit 10;

-- Check task execution history
select * from information_schema.task_history
where task_name in ('GENERATE_TASK', 'PROCESS_FILES_TASK', 'REFINE_TASK')
order by scheduled_time desc
limit 20;

-- Question 6.1: What is the difference between INFORMATION_SCHEMA and ACCOUNT_USAGE?
-- Answer: INFORMATION_SCHEMA provides real-time metadata for the current session/database with limited history (7-14 days). ACCOUNT_USAGE provides historical account-level metadata with up to 365 days of history but has a 45-minute to 3-hour latency for updates.

-- Exercise 6.2: Analyze data flow
-- DIFFICULTY: ADVANCED
-- TODO: Create queries to understand data flow

-- Count records in each table (staging, view, stream, analytical)
select 'CC_TRANS_STAGING' as table_name, count(*) as record_count from CC_TRANS_STAGING
union all
select 'CC_TRANS_STAGING_VIEW' as table_name, count(*) as record_count from CC_TRANS_STAGING_VIEW
union all
select 'CC_TRANS_STAGING_VIEW_STREAM' as table_name, count(*) as record_count from CC_TRANS_STAGING_VIEW_STREAM
union all
select 'CC_TRANS_ALL' as table_name, count(*) as record_count from CC_TRANS_ALL;

-- Find the latest transaction timestamp
select max(transaction_timestamp) as latest_transaction
from CC_TRANS_ALL;

-- Analyze transaction patterns (approved vs rejected)
select 
    approved,
    count(*) as transaction_count,
    avg(amount) as avg_amount,
    min(amount) as min_amount,
    max(amount) as max_amount,
    sum(amount) as total_amount
from CC_TRANS_ALL
group by approved
order by approved;

-- ===========================================
-- SECTION 7: ADVANCED ORCHESTRATION (EXPERT LEVEL)
-- ===========================================

-- Exercise 7.1: Create task dependencies
-- DIFFICULTY: EXPERT
-- TODO: Create a sequential task pipeline

-- Create tasks for pipeline 2
create or replace task GENERATE_TASK_P2
    warehouse = Orchestration_WH
as
    call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'cc_trans_p2', 25);

create or replace task PROCESS_FILES_TASK_P2
    warehouse = Orchestration_WH
as
    copy into CC_TRANS_STAGING from @CC_STAGE
    pattern = '.*cc_trans_p2.*'
    file_format = (type = JSON)
    force = true;

create or replace task REFINE_TASK_P2
    warehouse = Orchestration_WH
as
    insert into CC_TRANS_ALL 
    (element, object_type, transaction_id, transaction_type, amount, currency, 
     transaction_timestamp, approved, card_number, merchant_id)
    select 
        element, object_type, transaction_id, transaction_type, amount, currency,
        transaction_timestamp, approved, card_number, merchant_id
    from CC_TRANS_STAGING_VIEW_STREAM
    where metadata$action = 'INSERT';

-- Set up task dependencies using ALTER TASK ... ADD AFTER
alter task PROCESS_FILES_TASK_P2 add after GENERATE_TASK_P2;
alter task REFINE_TASK_P2 add after PROCESS_FILES_TASK_P2;

-- Create a root task that triggers the pipeline
create or replace task ROOT_TASK_P2
    warehouse = Orchestration_WH
    schedule = '5 minute'
as
    select 'Pipeline 2 triggered' as message;

alter task GENERATE_TASK_P2 add after ROOT_TASK_P2;

-- Question 7.1: How do task dependencies work in Snowflake?
-- Answer: Task dependencies create directed acyclic graphs (DAGs) where child tasks execute only after their parent tasks complete successfully. Dependencies are created using ALTER TASK ... ADD AFTER statements. Only the root task needs scheduling; child tasks are triggered by parent completion.

-- Exercise 7.2: Parallel processing
-- DIFFICULTY: EXPERT
-- TODO: Create tasks that can run in parallel

-- Create a wait task for parallel processing
create or replace task WAIT_TASK
    warehouse = Orchestration_WH
as
    select 'Waiting for parallel tasks to complete' as message;

-- Set up parallel task execution
create or replace task PARALLEL_TASK_A
    warehouse = Orchestration_WH
as
    call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'parallel_a', 10);

create or replace task PARALLEL_TASK_B
    warehouse = Orchestration_WH
as
    call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'parallel_b', 10);

-- Both parallel tasks depend on the same parent
alter task PARALLEL_TASK_A add after ROOT_TASK_P2;
alter task PARALLEL_TASK_B add after ROOT_TASK_P2;
-- Wait task depends on both parallel tasks
alter task WAIT_TASK add after PARALLEL_TASK_A;
alter task WAIT_TASK add after PARALLEL_TASK_B;

-- Monitor task dependencies
show tasks;
select * from information_schema.task_dependents order by task_name;

-- ===========================================
-- SECTION 8: CLEANUP AND BEST PRACTICES (EXPERT LEVEL)
-- ===========================================

-- Exercise 8.1: Task management
-- DIFFICULTY: EXPERT
-- TODO: Properly manage task lifecycle

-- Suspend all running tasks
alter task GENERATE_TASK suspend;
alter task PROCESS_FILES_TASK suspend;
alter task REFINE_TASK suspend;
alter task ROOT_TASK_P2 suspend;
alter task GENERATE_TASK_P2 suspend;
alter task PROCESS_FILES_TASK_P2 suspend;
alter task REFINE_TASK_P2 suspend;
alter task PARALLEL_TASK_A suspend;
alter task PARALLEL_TASK_B suspend;
alter task WAIT_TASK suspend;

-- Show all tasks and their states
show tasks;
select task_name, state, schedule, warehouse from information_schema.tasks
where task_schema = 'PUBLIC'
order by task_name;

-- Create a cleanup script
create or replace procedure CLEANUP_TASKS()
returns string
language sql
as
$$
begin
    -- Suspend all tasks
    alter task if exists GENERATE_TASK suspend;
    alter task if exists PROCESS_FILES_TASK suspend;
    alter task if exists REFINE_TASK suspend;
    alter task if exists ROOT_TASK_P2 suspend;
    alter task if exists GENERATE_TASK_P2 suspend;
    alter task if exists PROCESS_FILES_TASK_P2 suspend;
    alter task if exists REFINE_TASK_P2 suspend;
    alter task if exists PARALLEL_TASK_A suspend;
    alter task if exists PARALLEL_TASK_B suspend;
    alter task if exists WAIT_TASK suspend;
    
    return 'All tasks suspended successfully';
end;
$$;

-- Question 8.1: Why is it important to suspend tasks when not needed?
-- Answer: Suspending tasks prevents unnecessary compute costs, reduces warehouse usage, avoids resource contention, prevents unwanted data processing, and eliminates potential errors from running tasks in development/testing environments. It's essential for cost optimization and system maintenance.

-- Exercise 8.2: Performance analysis
-- DIFFICULTY: EXPERT
-- TODO: Analyze the performance of your pipeline

-- Calculate total processing time
select 
    task_name,
    avg(datediff('second', scheduled_time, completed_time)) as avg_execution_time_seconds,
    max(datediff('second', scheduled_time, completed_time)) as max_execution_time_seconds,
    min(datediff('second', scheduled_time, completed_time)) as min_execution_time_seconds
from information_schema.task_history
where state = 'SUCCEEDED'
group by task_name
order by avg_execution_time_seconds desc;

-- Analyze data volume processed
select 
    table_name,
    sum(rows_loaded) as total_rows_loaded,
    sum(bytes_loaded) as total_bytes_loaded,
    count(*) as load_operations
from information_schema.load_history
where table_name in ('CC_TRANS_STAGING', 'CC_TRANS_ALL')
group by table_name;

-- Identify potential bottlenecks
select 
    task_name,
    state,
    count(*) as execution_count,
    avg(datediff('second', scheduled_time, completed_time)) as avg_duration
from information_schema.task_history
group by task_name, state
order by task_name, state;

-- ===========================================
-- SECTION 9: COMPREHENSIVE DATA QUALITY CHECKS (MASTER LEVEL)
-- ===========================================

-- Exercise 9.1: Basic data quality validation
-- DIFFICULTY: INTERMEDIATE
-- TODO: Implement comprehensive data quality checks

-- Check for duplicate transactions
select 
    transaction_id,
    count(*) as duplicate_count
from CC_TRANS_ALL
group by transaction_id
having count(*) > 1
order by duplicate_count desc;

-- Validate data types and ranges
select 
    'Amount Validation' as check_type,
    count(*) as total_records,
    sum(case when amount < 0 then 1 else 0 end) as negative_amounts,
    sum(case when amount > 10000 then 1 else 0 end) as excessive_amounts,
    sum(case when amount is null then 1 else 0 end) as null_amounts
from CC_TRANS_ALL
union all
select 
    'Transaction ID Validation' as check_type,
    count(*) as total_records,
    sum(case when transaction_id < 1000000000 then 1 else 0 end) as invalid_format,
    sum(case when transaction_id > 9999999999 then 1 else 0 end) as out_of_range,
    sum(case when transaction_id is null then 1 else 0 end) as null_ids
from CC_TRANS_ALL;

-- Check for missing values
select 
    sum(case when transaction_id is null then 1 else 0 end) as null_transaction_id,
    sum(case when amount is null then 1 else 0 end) as null_amount,
    sum(case when currency is null then 1 else 0 end) as null_currency,
    sum(case when card_number is null then 1 else 0 end) as null_card_number,
    sum(case when merchant_id is null then 1 else 0 end) as null_merchant_id,
    sum(case when transaction_timestamp is null then 1 else 0 end) as null_timestamp
from CC_TRANS_ALL;

-- Validate card number format
select 
    count(*) as total_cards,
    sum(case when length(card_number::string) < 15 then 1 else 0 end) as short_card_numbers,
    sum(case when length(card_number::string) > 19 then 1 else 0 end) as long_card_numbers,
    sum(case when card_number < 0 then 1 else 0 end) as negative_card_numbers
from CC_TRANS_ALL;

-- Check for data anomalies
select 
    'Transaction Type Distribution' as anomaly_check,
    transaction_type,
    count(*) as count,
    count(*) * 100.0 / sum(count(*)) over() as percentage
from CC_TRANS_ALL
group by transaction_type
union all
select 
    'Currency Distribution' as anomaly_check,
    currency,
    count(*) as count,
    count(*) * 100.0 / sum(count(*)) over() as percentage
from CC_TRANS_ALL
group by currency
order by anomaly_check, count desc;

-- Exercise 9.2: Advanced data quality metrics
-- DIFFICULTY: ADVANCED
-- TODO: Create comprehensive data quality dashboard

-- Create data quality metrics table
create or replace table DATA_QUALITY_METRICS (
    metric_name string,
    metric_category string,
    metric_value number(10,4),
    total_records number,
    measurement_timestamp timestamp default current_timestamp(),
    table_name string
);

-- Calculate completeness metrics
insert into DATA_QUALITY_METRICS 
select 
    'COMPLETENESS_TRANSACTION_ID' as metric_name,
    'COMPLETENESS' as metric_category,
    (count(*) - sum(case when transaction_id is null then 1 else 0 end)) * 100.0 / count(*) as metric_value,
    count(*) as total_records,
    current_timestamp() as measurement_timestamp,
    'CC_TRANS_ALL' as table_name
from CC_TRANS_ALL
union all
select 
    'COMPLETENESS_AMOUNT',
    'COMPLETENESS',
    (count(*) - sum(case when amount is null then 1 else 0 end)) * 100.0 / count(*),
    count(*),
    current_timestamp(),
    'CC_TRANS_ALL'
from CC_TRANS_ALL
union all
select 
    'COMPLETENESS_CARD_NUMBER',
    'COMPLETENESS',
    (count(*) - sum(case when card_number is null then 1 else 0 end)) * 100.0 / count(*),
    count(*),
    current_timestamp(),
    'CC_TRANS_ALL'
from CC_TRANS_ALL;

-- Calculate validity metrics
insert into DATA_QUALITY_METRICS 
select 
    'VALIDITY_AMOUNT_RANGE' as metric_name,
    'VALIDITY' as metric_category,
    sum(case when amount between 0 and 10000 then 1 else 0 end) * 100.0 / count(*) as metric_value,
    count(*) as total_records,
    current_timestamp() as measurement_timestamp,
    'CC_TRANS_ALL' as table_name
from CC_TRANS_ALL
union all
select 
    'VALIDITY_CARD_NUMBER_FORMAT',
    'VALIDITY',
    sum(case when length(card_number::string) between 15 and 19 then 1 else 0 end) * 100.0 / count(*),
    count(*),
    current_timestamp(),
    'CC_TRANS_ALL'
from CC_TRANS_ALL;

-- Calculate consistency metrics
insert into DATA_QUALITY_METRICS 
select 
    'CONSISTENCY_CURRENCY_USD' as metric_name,
    'CONSISTENCY' as metric_category,
    sum(case when currency = 'USD' then 1 else 0 end) * 100.0 / count(*) as metric_value,
    count(*) as total_records,
    current_timestamp() as measurement_timestamp,
    'CC_TRANS_ALL' as table_name
from CC_TRANS_ALL
union all
select 
    'CONSISTENCY_TRANSACTION_TYPES',
    'CONSISTENCY',
    sum(case when transaction_type in ('PURCHASE', 'REFUND') then 1 else 0 end) * 100.0 / count(*),
    count(*),
    current_timestamp(),
    'CC_TRANS_ALL'
from CC_TRANS_ALL;

-- Create data quality dashboard
create or replace view DATA_QUALITY_DASHBOARD as
select 
    metric_category,
    metric_name,
    metric_value,
    case 
        when metric_value >= 95 then 'EXCELLENT'
        when metric_value >= 90 then 'GOOD'
        when metric_value >= 80 then 'ACCEPTABLE'
        when metric_value >= 70 then 'POOR'
        else 'CRITICAL'
    end as quality_rating,
    total_records,
    measurement_timestamp
from DATA_QUALITY_METRICS
order by metric_category, metric_name;

-- Exercise 9.3: Data quality monitoring and alerting
-- DIFFICULTY: EXPERT
-- TODO: Implement automated data quality monitoring

-- Create data quality monitoring procedure
create or replace procedure DATA_QUALITY_MONITOR()
returns string
language sql
as
$$
begin
    -- Clear previous metrics
    delete from DATA_QUALITY_METRICS where measurement_timestamp < current_timestamp() - interval '1 day';
    
    -- Calculate fresh metrics
    insert into DATA_QUALITY_METRICS 
    select 
        'COMPLETENESS_TRANSACTION_ID' as metric_name,
        'COMPLETENESS' as metric_category,
        (count(*) - sum(case when transaction_id is null then 1 else 0 end)) * 100.0 / count(*) as metric_value,
        count(*) as total_records,
        current_timestamp() as measurement_timestamp,
        'CC_TRANS_ALL' as table_name
    from CC_TRANS_ALL;
    
    -- Check for critical quality issues
    let critical_count number := (select count(*) from DATA_QUALITY_METRICS 
                                 where metric_value < 70 and measurement_timestamp > current_timestamp() - interval '1 hour');
    
    if (critical_count > 0) then
        return 'ALERT: ' || critical_count || ' critical data quality issues detected!';
    else
        return 'Data quality monitoring completed successfully';
    end if;
end;
$$;

-- Create automated quality check task
create or replace task DATA_QUALITY_CHECK_TASK
    warehouse = Orchestration_WH
    schedule = '15 minute'
as
    call DATA_QUALITY_MONITOR();

-- Implement quality alerting system
create or replace view QUALITY_ALERTS as
select 
    metric_name,
    metric_value,
    'CRITICAL' as alert_level,
    'Metric below 70% threshold' as alert_message,
    measurement_timestamp
from DATA_QUALITY_METRICS
where metric_value < 70
union all
select 
    metric_name,
    metric_value,
    'WARNING' as alert_level,
    'Metric below 80% threshold' as alert_message,
    measurement_timestamp
from DATA_QUALITY_METRICS
where metric_value between 70 and 80
order by alert_level, measurement_timestamp desc;

-- Create quality trend analysis
create or replace view QUALITY_TRENDS as
select 
    metric_name,
    metric_category,
    metric_value,
    lag(metric_value) over (partition by metric_name order by measurement_timestamp) as previous_value,
    metric_value - lag(metric_value) over (partition by metric_name order by measurement_timestamp) as trend_change,
    measurement_timestamp
from DATA_QUALITY_METRICS
order by metric_name, measurement_timestamp desc;

-- ===========================================
-- SECTION 10: PII PROTECTION AND DATA MASKING (MASTER LEVEL)
-- ===========================================

-- Exercise 10.1: Identify PII in credit card data
-- DIFFICULTY: INTERMEDIATE
-- TODO: Identify and categorize PII fields

-- Identify PII fields in the data
select 
    'CARD_NUMBER' as field_name,
    'Personal payment information' as pii_description,
    'HIGH' as sensitivity_level,
    'Payment Card Industry (PCI) regulated' as regulation
union all
select 'TRANSACTION_ID', 'Transaction identifier', 'MEDIUM', 'Business sensitive'
union all
select 'MERCHANT_ID', 'Merchant identifier', 'LOW', 'Business identifier'
union all
select 'AMOUNT', 'Transaction amount', 'MEDIUM', 'Financial information'
union all
select 'TRANSACTION_TIMESTAMP', 'Transaction timing', 'LOW', 'Behavioral data';

-- Create PII classification table
create or replace table PII_CLASSIFICATION (
    field_name string,
    data_type string,
    sensitivity_level string,
    pii_category string,
    masking_required boolean,
    retention_period_days number,
    regulatory_requirement string
);

-- Classify all fields by sensitivity
insert into PII_CLASSIFICATION values
('CARD_NUMBER', 'BIGINT', 'HIGH', 'PAYMENT_INSTRUMENT', true, 2555, 'PCI-DSS'),
('TRANSACTION_ID', 'BIGINT', 'MEDIUM', 'TRANSACTION_DATA', false, 2555, 'SOX'),
('AMOUNT', 'NUMBER', 'MEDIUM', 'FINANCIAL_DATA', false, 2555, 'SOX'),
('MERCHANT_ID', 'BIGINT', 'LOW', 'BUSINESS_DATA', false, 3650, 'NONE'),
('TRANSACTION_TIMESTAMP', 'TIMESTAMP', 'LOW', 'BEHAVIORAL_DATA', false, 1095, 'GDPR'),
('APPROVED', 'BOOLEAN', 'MEDIUM', 'TRANSACTION_DATA', false, 2555, 'SOX'),
('TRANSACTION_TYPE', 'STRING', 'LOW', 'TRANSACTION_DATA', false, 2555, 'NONE'),
('CURRENCY', 'STRING', 'LOW', 'REFERENCE_DATA', false, 3650, 'NONE');

-- Determine masking requirements
select 
    field_name,
    sensitivity_level,
    pii_category,
    case 
        when sensitivity_level = 'HIGH' then 'FULL_MASKING'
        when sensitivity_level = 'MEDIUM' then 'PARTIAL_MASKING'
        else 'NO_MASKING'
    end as masking_strategy,
    case 
        when pii_category = 'PAYMENT_INSTRUMENT' then 'Show only last 4 digits'
        when pii_category = 'FINANCIAL_DATA' then 'Round to nearest 100'
        when pii_category = 'TRANSACTION_DATA' then 'Hash or tokenize'
        else 'No masking required'
    end as masking_method
from PII_CLASSIFICATION
order by 
    case sensitivity_level 
        when 'HIGH' then 1 
        when 'MEDIUM' then 2 
        else 3 
    end;

-- Exercise 10.2: Implement data masking for PII
-- DIFFICULTY: ADVANCED
-- TODO: Create masked views for different user roles

-- Create masked view for analysts
create or replace view CC_TRANS_ANALYST_VIEW as
select 
    element,
    object_type,
    transaction_id,
    transaction_type,
    round(amount, -2) as amount, -- Round to nearest 100 for privacy
    currency,
    date_trunc('day', transaction_timestamp) as transaction_date, -- Remove time precision
    approved,
    concat('****-****-****-', right(card_number::string, 4)) as masked_card_number,
    merchant_id,
    loaded_at
from CC_TRANS_ALL;

-- Create masked view for auditors
create or replace view CC_TRANS_AUDITOR_VIEW as
select 
    element,
    object_type,
    sha2(transaction_id::string) as hashed_transaction_id, -- Hash transaction ID
    transaction_type,
    case 
        when amount > 1000 then '>1000'
        when amount > 500 then '500-1000'
        when amount > 100 then '100-500'
        else '<100'
    end as amount_range, -- Bucket amounts for privacy
    currency,
    date_trunc('month', transaction_timestamp) as transaction_month, -- Reduce time precision
    approved,
    'MASKED' as card_number, -- Fully mask card numbers
    merchant_id,
    loaded_at
from CC_TRANS_ALL;

-- Implement role-based access control
create role if not exists ANALYST_ROLE;
create role if not exists AUDITOR_ROLE;
create role if not exists DATA_ADMIN_ROLE;

grant select on view CC_TRANS_ANALYST_VIEW to role ANALYST_ROLE;
grant select on view CC_TRANS_AUDITOR_VIEW to role AUDITOR_ROLE;
grant all privileges on table CC_TRANS_ALL to role DATA_ADMIN_ROLE;

-- Deny direct access to sensitive table
revoke select on table CC_TRANS_ALL from role ANALYST_ROLE;
revoke select on table CC_TRANS_ALL from role AUDITOR_ROLE;

-- Test masking effectiveness
select 'Original Data Sample' as data_type, card_number, amount, transaction_timestamp
from CC_TRANS_ALL limit 3
union all
select 'Analyst View Sample' as data_type, masked_card_number, amount, transaction_date::timestamp
from CC_TRANS_ANALYST_VIEW limit 3
union all
select 'Auditor View Sample' as data_type, card_number, amount_range::number, transaction_month::timestamp
from CC_TRANS_AUDITOR_VIEW limit 3;

-- Exercise 10.3: Advanced PII protection strategies
-- DIFFICULTY: EXPERT
-- TODO: Implement advanced PII protection mechanisms

-- Create dynamic masking policy
create or replace masking policy CARD_NUMBER_MASK as (val bigint) returns bigint ->
    case 
        when current_role() in ('DATA_ADMIN_ROLE', 'ACCOUNTADMIN') then val
        when current_role() = 'ANALYST_ROLE' then parse_json(concat('****', right(val::string, 4)))::bigint
        else 0
    end;

create or replace masking policy AMOUNT_MASK as (val number) returns number ->
    case 
        when current_role() in ('DATA_ADMIN_ROLE', 'ACCOUNTADMIN') then val
        when current_role() = 'ANALYST_ROLE' then round(val, -2)
        when current_role() = 'AUDITOR_ROLE' then 
            case 
                when val > 1000 then 1500
                when val > 500 then 750
                when val > 100 then 300
                else 50
            end
        else 0
    end;

-- Apply masking to sensitive columns
alter table CC_TRANS_ALL modify column card_number set masking policy CARD_NUMBER_MASK;
alter table CC_TRANS_ALL modify column amount set masking policy AMOUNT_MASK;

-- Implement data retention policy
create or replace table DATA_RETENTION_POLICY (
    table_name string,
    retention_period_days number,
    last_cleanup_date date,
    next_cleanup_date date
);

insert into DATA_RETENTION_POLICY values
('CC_TRANS_ALL', 2555, current_date(), current_date() + 2555),
('CC_TRANS_STAGING', 30, current_date(), current_date() + 30),
('DATA_QUALITY_METRICS', 365, current_date(), current_date() + 365);

-- Create data anonymization procedure
create or replace procedure ANONYMIZE_EXPIRED_DATA()
returns string
language sql
as
$$
begin
    -- Delete data older than retention period
    delete from CC_TRANS_ALL 
    where transaction_timestamp < current_timestamp() - interval '7 years';
    
    -- Anonymize card numbers for data older than 2 years but within retention
    update CC_TRANS_ALL 
    set card_number = abs(hash(card_number)) % 9999999999999999
    where transaction_timestamp < current_timestamp() - interval '2 years'
    and transaction_timestamp >= current_timestamp() - interval '7 years';
    
    return 'Data anonymization completed successfully';
end;
$$;

-- Test PII protection mechanisms
select 
    'Protection Test' as test_type,
    current_role() as current_role,
    count(*) as accessible_records,
    avg(amount) as avg_visible_amount,
    min(card_number) as sample_card_number
from CC_TRANS_ALL;

-- Question 10.1: What are the key principles of PII protection in data systems?
-- Answer: The key principles include: 1) Data Minimization (collect only necessary data), 2) Purpose Limitation (use data only for intended purposes), 3) Access Control (restrict access based on roles), 4) Data Masking/Anonymization (protect sensitive data), 5) Retention Limits (delete data when no longer needed), 6) Audit Trails (track data access), 7) Encryption (protect data in transit/rest), and 8) Consent Management (respect user preferences).

-- Question 10.2: How would you implement GDPR compliance for this credit card data?
-- Answer: GDPR compliance requires: 1) Lawful basis for processing (contract performance for payments), 2) Data subject rights (access, rectification, erasure, portability), 3) Privacy by design (built-in protection), 4) Data Protection Impact Assessment (DPIA), 5) Breach notification (72 hours), 6) Data Protection Officer (DPO) if required, 7) International transfer safeguards, 8) Consent management for marketing, and 9) Regular compliance audits.

-- Question 10.3: What are the trade-offs between data utility and privacy protection?
-- Answer: Higher privacy protection often reduces data utility: 1) Masking reduces analytical precision, 2) Aggregation loses individual insights, 3) Anonymization may prevent longitudinal analysis, 4) Access controls limit data availability, 5) Retention limits reduce historical analysis. Balance requires risk assessment, stakeholder needs analysis, regulatory requirements, and implementing graduated protection levels based on sensitivity and business needs.

-- ===========================================
-- BONUS CHALLENGES
-- ===========================================

-- Challenge 1: Dagster integration with Dagster free trial and the token
-- Implementation approach:
-- 1. Set up Dagster Cloud free trial account
-- 2. Install dagster-snowflake library
-- 3. Configure Snowflake resource with connection details
-- 4. Create assets for each table (CC_TRANS_STAGING, CC_TRANS_ALL)
-- 5. Define dependencies between assets
-- 6. Schedule materialization using Dagster sensors
-- 7. Monitor data lineage and quality through Dagster UI
--
-- Example Dagster asset definition:
-- @asset(group_name="credit_card_pipeline")
-- def cc_trans_staging(context, snowflake: SnowflakeResource):
--     with snowflake.get_connection() as conn:
--         conn.execute("COPY INTO CC_TRANS_STAGING FROM @CC_STAGE")
--         return MaterializeResult(metadata={"rows_loaded": conn.fetchone()[0]})

-- Challenge 2: Implement data partitioning
-- Add partitioning to improve query performance

-- Create partitioned version of analytical table
create or replace table CC_TRANS_ALL_PARTITIONED (
    element int,
    object_type string,
    transaction_id bigint,
    transaction_type string,
    amount number(10,2),
    currency string,
    transaction_timestamp timestamp,
    approved boolean,
    card_number bigint,
    merchant_id bigint,
    loaded_at timestamp default current_timestamp(),
    transaction_date date generated always as (date(transaction_timestamp))
)
partition by (transaction_date);

-- Migrate data to partitioned table
insert into CC_TRANS_ALL_PARTITIONED 
(element, object_type, transaction_id, transaction_type, amount, currency, 
 transaction_timestamp, approved, card_number, merchant_id, loaded_at)
select 
    element, object_type, transaction_id, transaction_type, amount, currency,
    transaction_timestamp, approved, card_number, merchant_id, loaded_at
from CC_TRANS_ALL;

-- Create clustered table for better performance
create or replace table CC_TRANS_ALL_CLUSTERED (
    element int,
    object_type string,
    transaction_id bigint,
    transaction_type string,
    amount number(10,2),
    currency string,
    transaction_timestamp timestamp,
    approved boolean,
    card_number bigint,
    merchant_id bigint,
    loaded_at timestamp default current_timestamp()
)
cluster by (transaction_timestamp, merchant_id);

-- Performance comparison query
select 
    'Partitioned Table' as table_type,
    count(*) as record_count,
    avg(amount) as avg_amount
from CC_TRANS_ALL_PARTITIONED
where transaction_date >= current_date() - 7
union all
select 
    'Original Table' as table_type,
    count(*) as record_count,
    avg(amount) as avg_amount
from CC_TRANS_ALL
where date(transaction_timestamp) >= current_date() - 7;

-- Challenge 3: Create a data lineage tracking system
-- Track data flow from source to analytics

-- Create data lineage metadata table
create or replace table DATA_LINEAGE (
    lineage_id string,
    source_object string,
    target_object string,
    transformation_type string,
    transformation_logic string,
    dependency_level int,
    created_timestamp timestamp default current_timestamp(),
    last_updated timestamp
);

-- Insert lineage relationships
insert into DATA_LINEAGE values
('L001', '@CC_STAGE', 'CC_TRANS_STAGING', 'COPY', 'File copy from stage to table', 1, current_timestamp(), current_timestamp()),
('L002', 'CC_TRANS_STAGING', 'CC_TRANS_STAGING_VIEW', 'VIEW', 'JSON parsing and column mapping', 2, current_timestamp(), current_timestamp()),
('L003', 'CC_TRANS_STAGING_VIEW', 'CC_TRANS_STAGING_VIEW_STREAM', 'STREAM', 'Change data capture', 3, current_timestamp(), current_timestamp()),
('L004', 'CC_TRANS_STAGING_VIEW_STREAM', 'CC_TRANS_ALL', 'INSERT', 'Stream processing to analytical table', 4, current_timestamp(), current_timestamp()),
('L005', 'CC_TRANS_ALL', 'CC_TRANS_ANALYST_VIEW', 'VIEW', 'Data masking for analysts', 5, current_timestamp(), current_timestamp()),
('L006', 'CC_TRANS_ALL', 'CC_TRANS_AUDITOR_VIEW', 'VIEW', 'Data masking for auditors', 5, current_timestamp(), current_timestamp()),
('L007', 'CC_TRANS_ALL', 'DATA_QUALITY_METRICS', 'PROCEDURE', 'Quality metric calculation', 5, current_timestamp(), current_timestamp());

-- Create lineage visualization view
create or replace view DATA_LINEAGE_TREE as
with recursive lineage_hierarchy as (
    -- Base case: source objects
    select 
        lineage_id,
        source_object,
        target_object,
        transformation_type,
        dependency_level,
        source_object as root_source,
        concat(source_object, ' -> ', target_object) as lineage_path
    from DATA_LINEAGE
    where dependency_level = 1
    
    union all
    
    -- Recursive case: build the hierarchy
    select 
        dl.lineage_id,
        dl.source_object,
        dl.target_object,
        dl.transformation_type,
        dl.dependency_level,
        lh.root_source,
        concat(lh.lineage_path, ' -> ', dl.target_object)
    from DATA_LINEAGE dl
    join lineage_hierarchy lh on dl.source_object = lh.target_object
    where dl.dependency_level = lh.dependency_level + 1
)
select * from lineage_hierarchy
order by dependency_level, lineage_id;

-- Create impact analysis view
create or replace view IMPACT_ANALYSIS as
select 
    source_object,
    listagg(target_object, ', ') within group (order by dependency_level) as downstream_objects,
    count(*) as impact_count,
    max(dependency_level) as max_depth
from DATA_LINEAGE
group by source_object
order by impact_count desc;

-- Challenge 4: Implement real-time alerting
-- Create alerts for data anomalies

-- Create alerting configuration table
create or replace table ALERT_CONFIG (
    alert_id string,
    alert_name string,
    alert_type string,
    threshold_value number,
    comparison_operator string,
    target_table string,
    target_column string,
    alert_frequency_minutes number,
    notification_method string,
    is_active boolean,
    created_date timestamp default current_timestamp()
);

-- Configure various alerts
insert into ALERT_CONFIG values
('A001', 'High Value Transaction Alert', 'THRESHOLD', 5000, '>', 'CC_TRANS_ALL', 'AMOUNT', 5, 'EMAIL', true, current_timestamp()),
('A002', 'Low Approval Rate Alert', 'PERCENTAGE', 80, '<', 'CC_TRANS_ALL', 'APPROVED', 15, 'SLACK', true, current_timestamp()),
('A003', 'Data Quality Alert', 'PERCENTAGE', 90, '<', 'DATA_QUALITY_METRICS', 'METRIC_VALUE', 10, 'EMAIL', true, current_timestamp()),
('A004', 'Stream Processing Delay', 'COUNT', 1000, '>', 'CC_TRANS_STAGING_VIEW_STREAM', '*', 5, 'SMS', true, current_timestamp()),
('A005', 'Duplicate Transaction Alert', 'COUNT', 1, '>', 'CC_TRANS_ALL', 'TRANSACTION_ID', 30, 'EMAIL', true, current_timestamp());

-- Create alert monitoring procedure
create or replace procedure PROCESS_ALERTS()
returns string
language sql
as
$$
begin
    let alert_count number := 0;
    let alert_message string := '';
    
    -- Check high value transactions
    let high_value_count number := (select count(*) from CC_TRANS_ALL 
                                   where amount > 5000 and transaction_timestamp > current_timestamp() - interval '5 minutes');
    if (high_value_count > 0) then
        set alert_count = alert_count + 1;
        set alert_message = alert_message || 'HIGH VALUE ALERT: ' || high_value_count || ' transactions over $5000 detected. ';
    end if;
    
    -- Check approval rate
    let approval_rate number := (select avg(case when approved then 100.0 else 0.0 end) from CC_TRANS_ALL 
                                where transaction_timestamp > current_timestamp() - interval '15 minutes');
    if (approval_rate < 80) then
        set alert_count = alert_count + 1;
        set alert_message = alert_message || 'APPROVAL RATE ALERT: Rate dropped to ' || approval_rate || '%. ';
    end if;
    
    -- Check stream backlog
    let stream_count number := (select count(*) from CC_TRANS_STAGING_VIEW_STREAM);
    if (stream_count > 1000) then
        set alert_count = alert_count + 1;
        set alert_message = alert_message || 'STREAM BACKLOG ALERT: ' || stream_count || ' unprocessed records. ';
    end if;
    
    if (alert_count > 0) then
        return 'ALERTS TRIGGERED (' || alert_count || '): ' || alert_message;
    else
        return 'No alerts triggered - system operating normally';
    end if;
end;
$$;

-- Create alerting task
create or replace task REAL_TIME_ALERT_TASK
    warehouse = Orchestration_WH
    schedule = '2 minute'
as
    call PROCESS_ALERTS();

-- Create alert history table
create or replace table ALERT_HISTORY (
    alert_id string,
    alert_name string,
    alert_value number,
    threshold_value number,
    alert_timestamp timestamp default current_timestamp(),
    alert_message string,
    resolved_timestamp timestamp,
    resolved_by string
);

-- Test alerting system
call PROCESS_ALERTS();

-- Challenge 5: Optimize for cost
-- Implement cost optimization strategies

-- Create cost monitoring table
create or replace table COST_OPTIMIZATION_METRICS (
    metric_date date,
    warehouse_name string,
    credits_used number(10,6),
    query_count number,
    avg_execution_time_seconds number,
    total_bytes_scanned number,
    cost_per_query number(10,6),
    optimization_opportunities string
);

-- Cost optimization procedure
create or replace procedure ANALYZE_COSTS()
returns string
language sql
as
$$
begin
    -- Analyze warehouse usage
    insert into COST_OPTIMIZATION_METRICS
    select 
        current_date() as metric_date,
        warehouse_name,
        credits_used,
        query_count,
        avg_execution_time / 1000 as avg_execution_time_seconds,
        total_bytes_scanned,
        credits_used / nullif(query_count, 0) as cost_per_query,
        case 
            when avg_execution_time > 60000 then 'Consider query optimization'
            when credits_used / nullif(query_count, 0) > 0.1 then 'High cost per query'
            when total_bytes_scanned > 1000000000 then 'Consider result caching'
            else 'Operating efficiently'
        end as optimization_opportunities
    from snowflake.account_usage.warehouse_metering_history
    where start_time >= current_date() - 1
    group by warehouse_name, credits_used, query_count, avg_execution_time, total_bytes_scanned;
    
    return 'Cost analysis completed';
end;
$$;

-- Implement automatic warehouse scaling
create or replace procedure OPTIMIZE_WAREHOUSE_SIZE()
returns string
language sql
as
$$
begin
    -- Check current queue length and adjust warehouse size
    let queue_length number := (select count(*) from information_schema.query_history 
                               where execution_status = 'RUNNING' and warehouse_name = 'ORCHESTRATION_WH');
    
    if (queue_length > 5) then
        alter warehouse Orchestration_WH set warehouse_size = 'SMALL';
        return 'Scaled up warehouse due to high queue length';
    elseif (queue_length = 0) then
        alter warehouse Orchestration_WH set warehouse_size = 'XSMALL';
        return 'Scaled down warehouse due to low usage';
    else
        return 'Warehouse size optimal';
    end if;
end;
$$;

-- Create cost optimization recommendations
create or replace view COST_OPTIMIZATION_RECOMMENDATIONS as
select 
    'Warehouse Management' as category,
    'Enable auto-suspend on all warehouses' as recommendation,
    'HIGH' as priority,
    'Can reduce costs by up to 80% during idle periods' as benefit
union all
select 
    'Query Optimization',
    'Implement result caching for repeated queries',
    'MEDIUM',
    'Reduces compute costs for repetitive analytical queries'
union all
select 
    'Data Management',
    'Use clustering keys on large tables',
    'MEDIUM',
    'Improves query performance and reduces scan costs'
union all
select 
    'Task Scheduling',
    'Consolidate frequent tasks into fewer executions',
    'HIGH',
    'Reduces task overhead and warehouse startup costs'
union all
select 
    'Storage Optimization',
    'Implement data retention policies',
    'LOW',
    'Reduces storage costs for historical data'
union all
select 
    'Monitoring',
    'Set up cost alerts and budgets',
    'HIGH',
    'Prevents unexpected cost overruns';

-- Create automated cost optimization task
create or replace task COST_OPTIMIZATION_TASK
    warehouse = Orchestration_WH
    schedule = '60 minute'
as
    call OPTIMIZE_WAREHOUSE_SIZE();

-- Cost dashboard query
select 
    warehouse_name,
    sum(credits_used) as total_credits,
    avg(cost_per_query) as avg_cost_per_query,
    count(*) as optimization_checks,
    listagg(distinct optimization_opportunities, '; ') as recommendations
from COST_OPTIMIZATION_METRICS
where metric_date >= current_date() - 7
group by warehouse_name
order by total_credits desc;

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
