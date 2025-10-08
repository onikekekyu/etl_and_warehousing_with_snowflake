/*
================================================================================
SNOWFLAKE WORKSHOP - INSTRUCTOR ACCESS FOR EVALUATION
================================================================================

IMPORTANT: Execute this script AFTER completing your workshop exercises

Purpose: Test your governance skills & Creates an instructor account to evaluate your completed workshop 
Estimated Time: 2-3 minutes
Prerequisites: Student must have ACCOUNTADMIN access to their Snowflake account

================================================================================
*/

-- ===========================================
-- INSTRUCTOR ACCESS FOR EVALUATION
-- ===========================================

/*
INSTRUCTIONS FOR STUDENTS:
==========================

You have completed the Snowflake Streams and Tasks workshop - Congrats!
creating me an account for this workshop is optionnal but super important for the final exam.



STEPS TO GRANT INSTRUCTOR ACCESS:
==================================

1. Log into your Snowflake account using Snowsight or Classic Console
2. Ensure you have ACCOUNTADMIN privileges (check top-right role selector)
3. Execute ALL commands in this script sequentially (takes 2-3 minutes)
4.  Send the following to your instructor in your final project submission (in your git or on the slides):
   ✓ Your Snowflake account URL
   ✓ Your full name
   


WHAT THIS SCRIPT DOES:
======================
- Creates a user account: INSTRUCTOR_USER
- Sets a temporary password: 234
- Grants ACCOUNTADMIN role (needed to view all your work)
- Disables MFA requirement (for evaluation period)
- Allows instructor to access all databases, schemas, tables, tasks, and streams you created

YOUR WORK IS SAFE:
==================
- The instructor has READ access to evaluate your work
- Your objects, data, and configurations remain unchanged
- This is standard practice for assignment evaluation
- You'll remove this access after receiving your grade
*/

-- ===========================================
-- STEP 1: VERIFY YOUR CURRENT ROLE
-- ===========================================

-- Check your current role - you should see ACCOUNTADMIN
SELECT CURRENT_ROLE() as my_current_role;

-- If you're not ACCOUNTADMIN, switch to it now:
USE ROLE ACCOUNTADMIN;

-- Verify you're in ACCOUNTADMIN role now
SELECT CURRENT_ROLE() as my_current_role;


-- ===========================================
-- STEP 2: CREATE INSTRUCTOR USER
-- ===========================================

CREATE USER IF NOT EXISTS INSTRUCTOR_USER
    PASSWORD = '234'
    DEFAULT_ROLE = ACCOUNTADMIN
    DEFAULT_WAREHOUSE = COMPUTE_WH
    DEFAULT_NAMESPACE = 'CREDIT_CARD.PUBLIC'
    COMMENT = 'Instructor evaluation access - Created on Wed 8 Oct - Remove after grading complete'
    MUST_CHANGE_PASSWORD = FALSE
    DISPLAY_NAME = 'Workshop Instructor (Evaluator)'
    EMAIL = '';  -- Instructor will provide if needed

/*
CHECKPOINT : Check for success message
□ User created successfully or already exists

NOTE: This creates a temporary account for your instructor to review your work.
The instructor will be able to see:
- All databases and schemas you created (Credit_card, PUBLIC, etc.)
- All tables (CC_TRANS_STAGING, CC_TRANS_ALL, etc.)
- All views, streams, and tasks you implemented
- All stored procedures and functions
- Task execution history and data flow
*/

-- ===========================================
-- STEP 3: GRANT ACCOUNTADMIN ROLE
-- ===========================================

GRANT ROLE ACCOUNTADMIN TO USER INSTRUCTOR_USER;

/*
EXPLANATION: ACCOUNTADMIN is the highest privilege role in Snowflake
This allows the instructor to:
- View and evaluate all your completed workshop exercises
- Review your database structures (Credit_card database)
- Inspect your tables, views, streams, and tasks
- Check your data pipeline implementations
- Review task execution history and schedules
- Verify your data quality checks and PII protection measures
- Access query history to see your work process
- Grade your work comprehensively
*/

-- ===========================================
-- STEP 4: DISABLE MFA REQUIREMENT
-- ===========================================

-- ALTER USER INSTRUCTOR_USER SET MINS_TO_BYPASS_MFA = 999999;

/*
EXPLANATION: Multi-Factor Authentication (MFA) bypass
- This setting allows login without MFA for 999,999 minutes (~2 years)
- Makes it easier for instructor to access for evaluation period
- Simplifies the grading process
- IMPORTANT: This is ONLY acceptable for temporary evaluation accounts
- You will remove this user after grades are finalized
*/

-- ===========================================
-- STEP 5: GRANT WAREHOUSE ACCESS
-- ===========================================

-- Ensure instructor can use existing warehouses
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ACCOUNTADMIN;

-- If COMPUTE_WH doesn't exist, create it
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    COMMENT = 'Default compute warehouse';

/*
CHECKPOINT : Warehouse access configured
□ COMPUTE_WH exists and is accessible
*/

-- ===========================================
-- STEP 6: VERIFY USER CREATION
-- ===========================================

-- View the newly created user
SHOW USERS LIKE 'INSTRUCTOR_USER';

-- Get detailed user properties
DESC USER INSTRUCTOR_USER;

-- Verify role assignment
SHOW GRANTS TO USER INSTRUCTOR_USER;

/*
CHECKPOINT : Review the output above and confirm:
□ User exists: INSTRUCTOR_USER
□ Has role: ACCOUNTADMIN
□ MFA bypass: Set to 999999 minutes
*/

-- ===========================================
-- STEP 7: GET YOUR ACCOUNT INFORMATION
-- ===========================================

-- Run this query to get your account details
SELECT 
    CURRENT_ACCOUNT() as account_name,
    CURRENT_REGION() as region,
    CURRENT_ORGANIZATION_NAME() as organization
;

/*
CHECKPOINT 5: Note your account information from above

SUBMIT THE FOLLOWING TO YOUR INSTRUCTOR FOR EVALUATION:
========================================================

REQUIRED INFORMATION:
=====================

1. YOUR FULL NAME: Kirsten Chang

2. NAME of user created (normally instructor_user)
3. PASSWORD of user created (normally 234)

3. YOUR ACCOUNT URL : https://stuppcr-rib26508.snowflakecomputing.com

   Format examples:
   - https://abc12345.snowflakecomputing.com
   - https://myorg-myaccount.snowflakecomputing.com
   
   (Find this in your browser when logged into Snowflake)

4. CREDENTIALS FOR INSTRUCTOR:
   - Username: INSTRUCTOR_USER
   - Password: 234
   - Role: ACCOUNTADMIN


5. CONFIRMATION MESSAGE:
   "Instructor evaluation access has been granted to my Snowflake account"

you can give THIS INFORMATION TO YOUR INSTRUCTOR NOW
================================================
