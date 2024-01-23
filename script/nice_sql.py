import pandas as pd
from sqlalchemy import create_engine, text, types, bindparam
import re
import pyperclip
from mysql_info import task_engine


class Tables: 
    def __init__ (self, t1, t2, print_sql = True, clipboard = False, default_table = "stints.classify_by_program"):
        '''
        establishes settings and runs stints for the desired time period

        Parameters: 
            t1: start date, formatted as "YYYY-MM-DD"
            t2: end date, formatted as "YYYY-MM-DD"
            print_sql(Bool): whether to print the SQL statements when run, defaults to True
            clipboard(Bool): whether to copy the output table to your clipboard, defaults to False
            default_table: the source table to run queries on. defaults to "stints.classify_by_program"

        you'll need to modify the create_engine line the first time you use the code. for example:
        self.engine = create_engine('mysql+pymysql://eli:password@LCLCN092/tasks', isolation_level="AUTOCOMMIT")
        where LCLC is the handy label above my keyboard
        '''
        self.engine = task_engine
        self.t1 = t1
        self.t2 = t2
        self.q_t1 = f"'{self.t1}'"
        self.q_t2 = f"'{self.t2}'"
        self.table = default_table
        self.print = print_sql
        self.clipboard = clipboard
        self.joined_participants = False
        self.con = self.engine.connect().execution_options(autocommit=True)
        self.stints_run()

    def query_run(self, query):
        '''
        runs an SQL query (without a semicolon)
        format for a custom query: query_run(f"""query""")
        
        Parameters:
            query: SQL query
        '''
        query = text(f"{query};")
        if self.print_SQL is True:
            print(query)
        result = self.con.execute(query)
        data = result.fetchall()
        column_names = result.keys()
        df = pd.DataFrame(data, columns=column_names)
        if self.clipboard is True:
            df.to_clipboard()
        return df
    
    def query_modify(self, original_query, modification):
        '''
        edit a base SQL query, not particularly useful on its own

        Parameters:
            original_query: base SQL query
            modification: string to modify it
        '''
        # Use regular expressions to find the GROUP BY clause
        match = (re.compile(r"(?i)(\bGROUP\s+BY\b)",  re.IGNORECASE)).search(original_query)
        
        if match:
            # Insert the condition before the GROUP BY clause
            modified_query = original_query[:match.start()] + modification + ' ' + original_query[match.start():]
        else:
            # If no GROUP BY clause is found, just append the condition to the end of the query
            modified_query = original_query + ' ' + modification
        return modified_query


    def stints_run(self):
        '''
        runs the stints code
        '''
        stints_statements = f'''
DROP DATABASE IF EXISTS stints;
CREATE DATABASE stints;
SELECT 
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.pstatus1,
    ar.status_date1,
    ar.pstatus2,
    ar.status_date2,
    ar.pstatus3,
    ar.status_date3,
	ar.pstatus4,
    ar.status_date4
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE
	(
		(pstatus1 = 'Active' AND pstatus2 = 'Active') OR
		(pstatus2 = 'Active' AND pstatus3 = 'Active') OR 
		(pstatus3 = 'Active' AND pstatus4 = 'Active') OR 
		(pstatus1 = 'Closed' AND pstatus2 = 'Closed') OR
		(pstatus2 = 'Closed' AND pstatus3 = 'Closed') OR 
		(pstatus3 = 'Closed' AND pstatus4 = 'Closed')
	)
    AND
    (status_date1 REGEXP '2022|2023' OR status_date2 REGEXP '2022|2023' OR status_date3 REGEXP '2022|2023' OR status_date4 REGEXP '2022|2023');

SELECT 
	MAX(statuscount)
FROM 
	(SELECT 
		COUNT(status_id) AS statuscount
	FROM civicore.pstatus
	WHERE status_date REGEXP '2022|2023'
	GROUP BY participant_id) x
    ;
SELECT 
	'#2',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	(
		((ar.pstatus1 = 'Closed' AND (ar.status_date1 >= {self.q_t1} AND ar.status_date1 <= {self.q_t2})) AND (ar.pstatus2 = 'Active' AND ar.status_date2 < {self.q_t1})) OR
		((ar.pstatus2 = 'Closed' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2})) AND (ar.pstatus3 = 'Active' AND ar.status_date3 < {self.q_t1})) OR
		((ar.pstatus3 = 'Closed' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2})) AND (ar.pstatus4 = 'Active' AND ar.status_date4 < {self.q_t1})) OR
		((ar.pstatus4 = 'Closed' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2})) AND (ar.pstatus5 = 'Active' AND ar.status_date5 < {self.q_t1}))
    )
UNION
SELECT 
	'#5',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	(
		((ar.pstatus1 = 'Closed' AND (ar.status_date1 >= {self.q_t1} AND ar.status_date1 <= {self.q_t2})) AND (ar.pstatus2 = 'Active' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2}))) OR  
		((ar.pstatus2 = 'Closed' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2})) AND (ar.pstatus3 = 'Active' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2}))) OR 
		((ar.pstatus3 = 'Closed' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2})) AND (ar.pstatus4 = 'Active' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2}))) OR
		((ar.pstatus4 = 'Closed' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2})) AND (ar.pstatus5 = 'Active' AND (ar.status_date5 >= {self.q_t1} AND ar.status_date5 <= {self.q_t2}))) 
	)
UNION
SELECT 
	'#4',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)

WHERE 
	(ar.pstatus1 = 'Active' AND ar.status_date1 < {self.q_t1})
UNION
SELECT 
	'#7',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
    (ar.pstatus1 = 'Active' AND (ar.status_date1 >= {self.q_t1} AND ar.status_date1 <= {self.q_t2}))
UNION
SELECT 
	'#3',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE     
	(
		(ar.pstatus1 = 'Closed' AND ar.status_date1 > {self.q_t2}) AND (ar.pstatus2 = 'Active' AND ar.status_date2 < {self.q_t1}) OR
		(ar.pstatus2 = 'Closed' AND ar.status_date2 > {self.q_t2}) AND (ar.pstatus3 = 'Active' AND ar.status_date3 < {self.q_t1}) OR
		(ar.pstatus3 = 'Closed' AND ar.status_date3 > {self.q_t2}) AND (ar.pstatus4 = 'Active' AND ar.status_date4 < {self.q_t1}) OR
		(ar.pstatus4 = 'Closed' AND ar.status_date4 > {self.q_t2}) AND (ar.pstatus5 = 'Active' AND ar.status_date5 < {self.q_t1}) 
	)
UNION
SELECT 
	'#6',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE    
	(
		(ar.pstatus1 = 'Closed' AND ar.status_date1 > {self.q_t2}) AND (ar.pstatus2 = 'Active' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2})) OR
		(ar.pstatus2 = 'Closed' AND ar.status_date2 > {self.q_t2}) AND (ar.pstatus3 = 'Active' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2})) OR
		(ar.pstatus3 = 'Closed' AND ar.status_date3 > {self.q_t2}) AND (ar.pstatus4 = 'Active' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2})) OR
		(ar.pstatus4 = 'Closed' AND ar.status_date4 > {self.q_t2}) AND (ar.pstatus5 = 'Active' AND (ar.status_date5 >= {self.q_t1} AND ar.status_date5 <= {self.q_t2})) 
    )
UNION
SELECT
	'#8',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	(
		(ar.pstatus1 = 'Closed' AND ar.status_date1 > {self.q_t2}) AND (ar.pstatus2 = 'Active' AND ar.status_date2 > {self.q_t2}) OR 
		(ar.pstatus2 = 'Closed' AND ar.status_date2 > {self.q_t2}) AND (ar.pstatus3 = 'Active' AND ar.status_date3 > {self.q_t2}) OR
		(ar.pstatus3 = 'Closed' AND ar.status_date3 > {self.q_t2}) AND (ar.pstatus4 = 'Active' AND ar.status_date4 > {self.q_t2}) OR
		(ar.pstatus4 = 'Closed' AND ar.status_date4 > {self.q_t2}) AND (ar.pstatus5 = 'Active' AND ar.status_date5 > {self.q_t2})
    )
UNION
SELECT
	'#9',
    COUNT(DISTINCT participant_id),
    COUNT(participant_id)
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	(ar.pstatus1 = 'Active' AND ar.status_date1 > {self.q_t2})
;

CREATE TABLE stints.stints

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date2 AS active_date,
    ar.status_date1 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	((ar.pstatus1 = 'Closed' AND (ar.status_date1 >= {self.q_t1} AND ar.status_date1 <= {self.q_t2})) AND (ar.pstatus2 = 'Active' AND ar.status_date2 < {self.q_t1}))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date3 AS active_date,
    ar.status_date2 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	((ar.pstatus2 = 'Closed' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2})) AND (ar.pstatus3 = 'Active' AND ar.status_date3 < {self.q_t1}))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date4 AS active_date,
    ar.status_date3 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE        
	((ar.pstatus3 = 'Closed' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2})) AND (ar.pstatus4 = 'Active' AND ar.status_date4 < {self.q_t1}))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date5 AS active_date,
    ar.status_date4 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE
    ((ar.pstatus4 = 'Closed' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2})) AND (ar.pstatus5 = 'Active' AND ar.status_date5 < {self.q_t1}))    
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date2 AS active_date,
    ar.status_date1 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	((ar.pstatus1 = 'Closed' AND (ar.status_date1 >= {self.q_t1} AND ar.status_date1 <= {self.q_t2})) AND (ar.pstatus2 = 'Active' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2})))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date3 AS active_date,
    ar.status_date2 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE    
	((ar.pstatus2 = 'Closed' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2})) AND (ar.pstatus3 = 'Active' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2})))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date4 AS active_date,
    ar.status_date3 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE       
	((ar.pstatus3 = 'Closed' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2})) AND (ar.pstatus4 = 'Active' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2}))) 
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date5 AS active_date,
    ar.status_date4 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE      
	((ar.pstatus4 = 'Closed' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2})) AND (ar.pstatus5 = 'Active' AND (ar.status_date5 >= {self.q_t1} AND ar.status_date5 <= {self.q_t2}))) 
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date1 AS active_date,
    NULL AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
	(ar.pstatus1 = 'Active' AND ar.status_date1 < {self.q_t1})
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date1 AS active_date,
    NULL AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE 
    (ar.pstatus1 = 'Active' AND (ar.status_date1 >= {self.q_t1} AND ar.status_date1 <= {self.q_t2}))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date2 AS active_date,
    ar.status_date1 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE     
	(ar.pstatus1 = 'Closed' AND ar.status_date1 > {self.q_t2}) AND (ar.pstatus2 = 'Active' AND ar.status_date2 < {self.q_t1}) 
UNION
SELECT  																				
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date3 AS active_date,
    ar.status_date2 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE         
	(ar.pstatus2 = 'Closed' AND ar.status_date2 > {self.q_t2}) AND (ar.pstatus3 = 'Active' AND ar.status_date3 < {self.q_t1})
UNION
SELECT  																									
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date4 AS active_date,
    ar.status_date3 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE      
	(ar.pstatus3 = 'Closed' AND ar.status_date3 > {self.q_t2}) AND (ar.pstatus4 = 'Active' AND ar.status_date4 < {self.q_t1}) 
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date5 AS active_date,
    ar.status_date4 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE       
    (ar.pstatus4 = 'Closed' AND ar.status_date4 > {self.q_t2}) AND (ar.pstatus5 = 'Active' AND ar.status_date5 < {self.q_t1}) 
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date2 AS active_date,
    ar.status_date1 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE    
	(ar.pstatus1 = 'Closed' AND ar.status_date1 > {self.q_t2}) AND (ar.pstatus2 = 'Active' AND (ar.status_date2 >= {self.q_t1} AND ar.status_date2 <= {self.q_t2}))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date3 AS active_date,
    ar.status_date2 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE
	(ar.pstatus2 = 'Closed' AND ar.status_date2 > {self.q_t2}) AND (ar.pstatus3 = 'Active' AND (ar.status_date3 >= {self.q_t1} AND ar.status_date3 <= {self.q_t2}))
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date4 AS active_date,
    ar.status_date3 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE
	(ar.pstatus3 = 'Closed' AND ar.status_date3 > {self.q_t2}) AND (ar.pstatus4 = 'Active' AND (ar.status_date4 >= {self.q_t1} AND ar.status_date4 <= {self.q_t2})) 
UNION

SELECT
	ar.participant_id,
    p.first_name,
    p.last_name,
    ar.status_date5 AS active_date,
    ar.status_date4 AS closed_date
FROM statuses.all_rows ar
JOIN civicore.participants p USING (participant_id)
WHERE
	(ar.pstatus4 = 'Closed' AND ar.status_date4 > {self.q_t2}) AND (ar.pstatus5 = 'Active' AND (ar.status_date5 >= {self.q_t1} AND ar.status_date5 <= {self.q_t2}));
CREATE TABLE stints.stint_count

SELECT 
	s.participant_id,
    COUNT(participant_id) AS stint_count
FROM stints.stints s
GROUP BY participant_id;

CREATE TABLE stints.stints_plus_stint_count
SELECT *
FROM stints.stints
JOIN stints.stint_count sc USING (participant_id);

SELECT
	COUNT(participant_id),				
    COUNT(DISTINCT participant_id)		
FROM stints.stints_plus_stint_count;
        CREATE TABLE IF NOT EXISTS stints.stints_programs_grants
        SELECT 

			spsc.participant_id,
			p.first_name,
			p.last_name,
			stint_count,

			spsc.active_date,
			spsc.closed_date,
			p.program_type,
			g.grant_type,
			g.start_date,
			g.end_date,
			g.comments
		FROM stints.stints_plus_stint_count spsc
		JOIN civicore.participants p USING (participant_id)
		LEFT JOIN civicore.grant_type g USING (participant_id)
		WHERE ((grant_type REGEXP 'jisc|yip|a2j|scan|crws|idhs vp|jac hd' OR g.comments REGEXP 'crws' OR g.grant_type IS NULL) AND g.grant_type != 'SCaN Outreach') AND (g.end_date IS NULL OR g.end_date >= {self.q_t1} AND g.start_date <= {self.q_t2})
		ORDER BY participant_id, active_date;

CREATE TABLE stints.classify_by_program
SELECT *
FROM stints.stints_programs_grants spg;

ALTER TABLE stints.classify_by_program
ADD COLUMN service_legal INT AFTER comments,
ADD COLUMN service_cm INT AFTER service_legal,
ADD COLUMN cm_juv_divert INT AFTER service_cm,
ADD COLUMN cm_scan INT AFTER cm_juv_divert,
ADD COLUMN cm_scan_outreach INT AFTER cm_scan,
ADD COLUMN hd INT AFTER cm_scan_outreach,
ADD COLUMN hd_rct INT AFTER hd,
ADD COLUMN rjcc INT AFTER hd_rct,
ADD COLUMN crws INT AFTER rjcc,
ADD COLUMN vp INT after crws;
UPDATE stints.classify_by_program									
SET service_legal = 1
WHERE program_type REGEXP 'RCT' OR program_type REGEXP 'legal representation';

UPDATE stints.classify_by_program
SET service_cm = 1
WHERE program_type REGEXP 'RCT' OR program_type REGEXP 'case management' OR grant_type REGEXP 'scan' OR grant_type REGEXP 'jisc|yip';

UPDATE stints.classify_by_program
SET cm_juv_divert = 1
WHERE grant_type REGEXP 'JISC|YIP';

UPDATE stints.classify_by_program
SET cm_scan = 1
WHERE grant_type REGEXP 'dfss - scan';

UPDATE stints.classify_by_program
SET cm_scan_outreach = 1
WHERE grant_type REGEXP 'scan outreach';

UPDATE stints.classify_by_program
SET hd = 1
WHERE service_legal = 1 AND service_cm = 1;

UPDATE stints.classify_by_program
SET hd_rct = 1
WHERE program_type REGEXP 'RCT';

UPDATE stints.classify_by_program
SET rjcc = 1
WHERE program_type REGEXP 'rjcc';

UPDATE stints.classify_by_program
SET crws = 1
WHERE comments REGEXP 'crws' OR grant_type REGEXP 'crws';

UPDATE stints.classify_by_program
SET vp = 1
WHERE grant_type = 'IDHS VP' OR grant_type = 'JAC HD';
'''
        for statement in stints_statements.split(';'):
            if statement.strip():
                self.con.execute(text(statement.strip() + ';'))

    def table_update(self, desired_table = None, update_default_table = False):
        """
        update a table of interest in SQL

        Parameters:
            desired_table: the table to update ("a2j", "mreport", "classify_concat", "jac", "idhs")
            update_default_table: if True, changes the active table from stints.classify_by_program to the updated table
        """
        if desired_table.lower() == "mreport":
            statements = f'''drop table if exists participants.mreport;
create table participants.mreport as(
select *,
TIMESTAMPDIFF(DAY, active_date, {self.q_t2}),
case 
when (closed_date IS NULL AND TIMESTAMPDIFF(DAY, active_date, {self.q_t2}) BETWEEN 0 AND 45) or (closed_date IS NOT NULL AND TIMESTAMPDIFF(DAY, active_date, closed_date) BETWEEN 0 AND 45)then "0-45"
when (closed_date IS NULL AND TIMESTAMPDIFF(DAY, active_date, {self.q_t2}) BETWEEN 46 AND 90) or (closed_date IS NOT NULL AND TIMESTAMPDIFF(DAY, active_date, closed_date) BETWEEN 46 AND 90)then "46-90"
when (closed_date IS NULL AND TIMESTAMPDIFF(DAY, active_date, {self.q_t2}) BETWEEN 91 AND 180) or (closed_date IS NOT NULL AND TIMESTAMPDIFF(DAY, active_date, closed_date) BETWEEN 91 AND 180)then "91-180"
when (closed_date IS NULL AND TIMESTAMPDIFF(DAY, active_date, {self.q_t2}) BETWEEN 181 AND 270) or (closed_date IS NOT NULL AND TIMESTAMPDIFF(DAY, active_date, closed_date) BETWEEN 181 AND 270)then "181-270"
else "271+" end as "days_served",
case
    when closed_date <= {self.q_t2} then "closed out"
    else "active client"
    end as "client_status",
case
	when active_date >= {self.q_t1} then "new client"
    else NULL
    end as new_client,
case when {self.q_t1} < DATE_ADD(birth_date, INTERVAL 18 YEAR) then "juvenile" else "emerging adult" end as "age"
from stints.classify_by_program
join (select participant_id, birth_date, race, gender from civicore.participants) x using(participant_id)
);'''
            new_table = "participants.mreport"
            self.joined_participants = True
        if desired_table.lower() == "classify_concat":
            statements = f'''drop table if exists participants.classify_concat; create table participants.classify_concat as(
SELECT 
    participant_id,
    first_name,
    last_name,
    GROUP_CONCAT(DISTINCT program_type ORDER BY program_type) AS program_type,
    GROUP_CONCAT(DISTINCT active_date ORDER BY active_date) AS active_date,
    GROUP_CONCAT(DISTINCT closed_date ORDER BY closed_date) AS closed_date,
    GROUP_CONCAT(DISTINCT grant_type ORDER BY grant_type) AS grant_type,
    GROUP_CONCAT(DISTINCT service_legal ORDER BY service_legal) AS service_legal,
    GROUP_CONCAT(DISTINCT service_cm ORDER BY service_cm) AS service_cm,
    GROUP_CONCAT(DISTINCT cm_juv_divert ORDER BY cm_juv_divert) AS cm_juv_divert,
    GROUP_CONCAT(DISTINCT cm_scan ORDER BY cm_scan) AS cm_scan,
    GROUP_CONCAT(DISTINCT cm_scan_outreach ORDER BY cm_scan_outreach) AS cm_scan_outreach,
    GROUP_CONCAT(DISTINCT hd ORDER BY hd) AS hd,
    GROUP_CONCAT(DISTINCT hd_rct ORDER BY hd_rct) AS hd_rct,
    GROUP_CONCAT(DISTINCT rjcc ORDER BY rjcc) AS rjcc,
    GROUP_CONCAT(DISTINCT crws ORDER BY crws) AS crws,
    GROUP_CONCAT(DISTINCT vp ORDER BY vp) AS vp
FROM 
    stints.classify_by_program
GROUP BY 
    participant_id, first_name, last_name);'''
            new_table = "participants.classify_concat"
        if desired_table.lower() == "jac":
            statements = f'''drop table if exists participants.jac;
create table participants.jac as(
select *,
	case when active_date between {self.q_t1} and {self.q_t2} then "new" else "continuing" end as new_client
from (select participant_id, max(active_date) as active_date from stints.classify_by_program
where grant_type regexp ".*JAC HD.*" or program_type regexp ".*JAC.*"
group by participant_id) p
join stints.classify_by_program using(participant_id, active_date));'''
            new_table = "participants.idhs"
        if desired_table.lower() == "idhs":
            statements = f'''drop table if exists participants.idhs;
create table participants.idhs as(
select *,
	case when active_date between {self.q_t1} and {self.q_t2} then "new" else "continuing" end as new_client
from (select participant_id, max(active_date) as active_date from stints.classify_by_program
where grant_type regexp ".*JAC HD.*" or program_type regexp ".*JAC.*"
group by participant_id) p
join stints.classify_by_program using(participant_id, active_date));'''
            new_table = "participants.idhs"
        if desired_table.lower() == "a2j":
            statements = f'''drop table participants.a2j;
create table participants.a2j as(

with ranked_addresses as(select first_name, last_name, a.*,
ROW_NUMBER() OVER (partition by participant_id ORDER BY primary_address DESC, entered_date DESC) AS rn from participants.polk_concat
join civicore.address a using(participant_id)
),
a2j as (select * from stints.classify_by_program 
where grant_type like "%A2J%"),
recent_legal as(SELECT a.participant_id AS participant_id, group_concat(distinct a.arrest_date) AS arrest_date, a.juvenile_adult, a.case_type
FROM (
    SELECT
        participant_id,
        MAX(arrest_date) AS arrest_date,
        GROUP_CONCAT(DISTINCT juvenile_adult SEPARATOR ', ') AS juvenile_adult,
        GROUP_CONCAT(DISTINCT case_type SEPARATOR ', ') AS case_type
    FROM civicore.legal
    GROUP BY participant_id
) AS a
INNER JOIN civicore.legal AS l
ON a.participant_id = l.participant_id AND a.arrest_date = l.arrest_date
group by participant_id, juvenile_adult, case_type)

select p.first_name, p.last_name, p.participant_id, p.gender, p.race, 
	case when p.race = "Hispanic/Latinx" then "Yes" else "Not Reported" end as "Hispanic/Latino", 
    a.zip, o.juvenile_adult, o.case_type
from a2j a2j
join civicore.participants p using(participant_id)
join (select * from ranked_addresses where rn = 1) a using(participant_id)
left join recent_legal o using(participant_id)
);'''
            new_table = "participants.a2j"
            self.joined_participants = True
        for statement in statements.split(';'):
            if statement.strip():
                self.con.execute(text(statement.strip() + ';'))
        if update_default_table is True:
            self.table = new_table
    
    def run_report(self, func_dict = None,*args, **kwargs):
        '''
        runs a desired report
        Parameters:
            func_dict: dictionary of functions to include, defaults to self.report_funcs. To use a different  
        '''
        if func_dict is None:
            func_dict = self.report_funcs
        
        result_dict = {}
        for result_key, (func_name, func_args) in func_dict.items():
            func = getattr(self, func_name, None)
            if func and callable(func):
                result = func(*func_args, *args, **kwargs)
                result_dict[result_key] = result
        clipboard_content = []
        for df_name, df in result_dict.items():
            df_content = df.to_csv(index=False, sep='\t')
            content_with_name = f"{df_name}\n{df_content}\n\n"
            clipboard_content.append(content_with_name)

        # Combine the content and copy to clipboard
        all_content = "\n".join(clipboard_content)
        pyperclip.copy(all_content)
        return(all_content)
    
    def percentage_convert(self, df, replace = True):
        '''
        converts numeric columns to percentages

        Parameters:
            Replace (bool): True replaces a column with its percentages, False adds a new column. Defaults to True
        '''
        result_df = df.copy()
        for column in result_df.columns:
            try:
                if pd.api.types.is_numeric_dtype(result_df[column]):
                    total_value = result_df[column].sum()
                    percentage_column = (result_df[column] / total_value) * 100
                    percentage_column = percentage_column.round(2)
                    percentage_column = percentage_column.astype(str) + "%"
                    if not replace:
                        result_df[f'percentage_{column}'] = percentage_column
                        result_df.at['Total', f'percentage_{column}'] = total_value
                    else:
                        result_df[column] = percentage_column
                    result_df.at['Total', column] = total_value  # Add "Total" as the final row in each column
            except TypeError as e:
                print(f"Error in column '{column}'")

        return result_df


class Queries(Tables):
    '''
    Sets up a table for a given timeframe

    Parameters:
        t1: start date, formatted as "YYYY-MM-DD"
        t2: end date, formatted as "YYYY-MM-DD"
        print_sql (Bool): whether to print the SQL statements when run, defaults to True
        clipboard (Bool): whether to copy the output table to your clipboard, defaults to False
        default_table: the source table to run queries on. defaults to "stints.classify_by_program"
    
    '''
    def dem_age(self, new_clients = False, tally = True, age = 18, cutoff_date = "active_date"):
        '''
        Returns a count of clients below/above a certain age threshold, or identifies clients as juveniles/adults 

        Parameters:
            new_clients (Bool): if true, only counts clients who began between t1 and t2. defaults to False
            tally (Bool): if true, returns a count of juv/adults, if false, returns a list. defaults to True
            age: threshold at which a client is counted as a juvenile, defaults to 18
            cutoff_date: time period at which to calculate age. defaults to "active_date", but one could also use a different column, "t1", or a date in "YYYY-MM-DD" format

            
        SQL Equivalent:
        
            select 
                count(distinct case when active_date< DATE_ADD(birth_date, INTERVAL 18 YEAR) then participant_id else null end) as 'Juvenile',
                count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 18 AND 25 then participant_id else null end) as 'Adult'
            from stints.classify_by_program
            join civicore.participants using(participant_id);'''
        if cutoff_date.lower() == "t1":
            cutoff_date = self.t1
        if cutoff_date.lower()  == "t2":
            cutoff_date = self.t2
        if cutoff_date.lower() != "active_date":
            cutoff_date = f'"{cutoff_date}"'
        if tally is True:
            query = f'''select 
                count(distinct case when {cutoff_date}< DATE_ADD(birth_date, INTERVAL {age} YEAR) then participant_id else null end) as 'Juvenile',
                count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, {cutoff_date}) BETWEEN {age} AND 25 then participant_id else null end) as 'Adult'
                from {self.table}
                join civicore.participants using(participant_id)'''
        else: 
            query = f'''select 
                l.*, (case when {cutoff_date}< DATE_ADD(birth_date, INTERVAL {age} YEAR) then 'Juvenile' else "Adult" end) as 'age_group'
                from {self.table} l
                join civicore.participants using(participant_id)'''
        modifier = f'''WHERE active_date between {self.q_t1} AND {self.q_t2}'''
        if new_clients is True:
            query = self.query_modify(str(query), modifier)
        df = self.query_run(query)
        return(df)
    
    def dem_race_gender(self, new_clients = False, race_gender = None):
        '''
        Returns a count of client races or genders

        Parameters:
            new_clients (Bool): if true, only counts clients who began between t1 and t2. defaults to False
            race_gender: the category to tally, enter either "race" or "gender"
        
        
        SQL Equivalent:

            select race, count(distinct participant_id)
            from stints.classify_by_program
            join civicore.participants using(participant_id)
            group by race;
        '''
        query = f'''select {race_gender}, count(distinct participant_id)
        from {self.table}
        join civicore.participants using(participant_id)
        group by {race_gender}'''
        modifier = f'''WHERE active_date between {self.q_t1} AND {self.q_t2}'''
        if new_clients is True:
            query = self.query_modify(str(query), modifier)  # Use self.query_modify here
        df = self.query_run(query)
        return(df)
    
    def dem_recent_address(self):
        '''
        Finds the recent addresses of clients


        SQL Equivalent:

            with ranked_addresses as
                (select first_name, last_name, a.*,
                    ROW_NUMBER() OVER (partition by participant_id 
                    ORDER BY primary_address DESC, entered_date DESC) AS rn 
                from stints.classify_by_program
                join civicore.address a using(participant_id))

                select * from ranked_addresses
                where rn = 1
                    ;
        '''
    def dem_recent_address(self, neighborhoods = False):
        '''
        Finds the recent addresses of clients.
        Parameters:
            neighborhoods: True if using a table with neighborhoods. Defaults to False
        '''
        if neighborhoods is False:
            query = f'''with ranked_addresses as(select first_name, last_name, a.*,
                    ROW_NUMBER() OVER (partition by participant_id ORDER BY primary_address DESC, entered_date DESC) AS rn from {self.table}
                    join civicore.address a using(participant_id))
                    select * from ranked_addresses
                    where rn = 1
                    '''
        else:
            query = f'''WITH ranked_addresses AS (
    SELECT
        first_name,
        last_name,
        a.*,
        ROW_NUMBER() OVER (
            PARTITION BY participant_id
            ORDER BY 
                primary_address DESC,
                entered_date DESC,
                CASE WHEN LOWER(neighborhood) not LIKE '%other%' THEN 1 ELSE 0 END
        ) AS rn
    FROM
        (select distinct first_name, last_name, participant_id from {self.table}) s
        JOIN tasks.neighborhoods a USING(participant_id)
)
SELECT participant_id, location, primary_address, entered_date, updated_date, neighborhood, rn
FROM ranked_addresses
'''
        df = self.query_run(query)
        return(df)
    
    def docs_has_formatted(self):
        '''
        checks if clients have ISPs or Assessments and if they are formatted correctly
        '''
        query = f'''SELECT DISTINCT (participant_id), ASSM_ISP_formatted, has_ASSM_ISP, FCS_formatted, has_FCS,  BP_formatted, has_BP, PCL_formatted, has_PCL,  has_cdc, assess
FROM (select * from {self.table}) cr
LEFT JOIN( select participant_id, first_name, last_name
from civicore.participants) p USING (participant_id)
LEFT JOIN(
select participant_id,
        COUNT(CASE
        WHEN d.document_name REGEXP '^.*_ASSM_ISP_[01][0-9][0-3][0-9][12][0-9]{3}$' THEN 1
        ELSE NULL END) AS ASSM_ISP_formatted,
    CASE
        WHEN d.document_name REGEXP '.*ASSM.*|.*ISP.*|.*Matrix.*|.*service.*' THEN 'Yes'
        ELSE 'No'
    END AS has_ASSM_ISP
 from documents.unfiltered d
WHERE d.document_name REGEXP '.*ASSM.*|.*ISP.*|.*Matrix.*|.*service.*'
GROUP BY participant_id, Has_ASSM_ISP
) assm USING (participant_id)
LEFT JOIN(
select participant_id,
        COUNT(CASE
        WHEN d.document_name REGEXP '^.*_FCS_[01][0-9][0-3][0-9][12][0-9]{3}$' THEN 1
        ELSE NULL END) AS FCS_formatted,
    CASE
        WHEN d.document_name REGEXP '.*FCS.*|.*Crime.*' THEN 'Yes'
        ELSE 'No'
    END AS has_FCS
 from documents.unfiltered d
WHERE d.document_name REGEXP '.*FCS.*|.*Crime.*'
GROUP BY participant_id, has_FCS
) fcs USING (participant_id)
LEFT JOIN(
select participant_id,
        COUNT(CASE
        WHEN d.document_name REGEXP '^.*_BP_[01][0-9][0-3][0-9][12][0-9]{3}$' THEN 1
        ELSE NULL END) AS BP_formatted,
    CASE
        WHEN d.document_name REGEXP '.*BP.*|.*Buss.*' THEN 'Yes'
        ELSE 'No'
    END AS has_BP
 from documents.unfiltered d
WHERE d.document_name REGEXP '.*BP.*|.*Buss.*'
GROUP BY participant_id, has_BP
) bp USING (participant_id)
LEFT JOIN(
select participant_id,
        COUNT(CASE
        WHEN d.document_name REGEXP '^.*_PCL_[01][0-9][0-3][0-9][12][0-9]{3}$' THEN 1
        ELSE NULL END) AS PCL_formatted,
    CASE
        WHEN d.document_name REGEXP '.*PCL.*|.*PLC.*|.*Post-Traumatic.*' THEN 'Yes'
        ELSE 'No'
    END AS has_PCL
 from documents.unfiltered d
WHERE d.document_name REGEXP '.*PCL.*|.*PLC.*|.*Post-Traumatic.*'
GROUP BY participant_id, has_PCL
) PCL USING (participant_id)
LEFT JOIN(
select participant_id,
        COUNT(CASE
        WHEN d.document_name REGEXP '^.*_CDC_[01][0-9][0-3][0-9][12][0-9]{3}$' THEN 1
        ELSE NULL END) AS CDC_formatted,
    CASE
        WHEN d.document_name REGEXP '.*CDC.*|.*Achievement.*|.*_AM*' THEN 'Yes'
        ELSE 'No'
    END AS has_CDC
 from documents.unfiltered d
WHERE d.document_name REGEXP '.*CDC.*|.*Achievement.*|.*_AM*'
GROUP BY participant_id, has_CDC
) cdc USING (participant_id)
left join(
select participant_id,
        COUNT(CASE
                WHEN d.document_type = 'Assessments' THEN 1 ELSE NULL END) as assess
 from documents.unfiltered d
 group by participant_id) asseses USING (participant_id);'''
        df = self.query_run(query)
        return df
    
    def grant_tally(self, start_end = None):
        '''
        counts how many clients are on a grant
        
        Parameters
            start_end: "start_date" returns clients who started in timeframe, "end_date" returns clients who ended
        
        '''
        query = f'''select grant_type, count(distinct participant_id) from {self.table} group by grant_type'''
        if start_end == "start_date":
            query = self.query_modify(str(query), f'''where ({start_end} between {self.q_t1} and {self.q_t2}) or active_date between {self.q_t1} and {self.q_t2}''')
        elif start_end == "end_date":
            query = self.query_modify(str(query), f'''where ({start_end} between {self.q_t1} and {self.q_t2}) or closed_date between {self.q_t1} and {self.q_t2}''')
        df = self.query_run(query)
        return df
    
    def incident_tally(self):
        '''
        counts incidents in timeframe
        '''
        query = f'''SELECT count(case when how_hear regexp '.*CPIC.*' then incident_id else null end) as CPIC,
	count(case when how_hear not regexp '.*CPIC.*' then incident_id else null end) as non_CPIC
FROM civicore.critical_incidents
where (date_incident between {self.q_t1} and {self.q_t2})'''
        df = self.query_run(query)
        return df
    
    def legal_tally(self, pending = False):
        '''
        Tallies clients with a case in CiviCore

        Parameters:
            pending (Bool): if True, tallies the clients with a case pending. Defaults to False. 

        
        SQL Equivalent:

            with mlegal as 
                (select * from 
                    (select participant_id, active_date, closed_date from stints.classify_by_program)k
                join civicore.legal l using(participant_id))
            select count(distinct participant_id)
            from mlegal where arrest_date < '2023-08-31';
        '''
        query = f'''with mlegal as (select * from (select participant_id, active_date, closed_date from {self.table})k
join civicore.legal l using(participant_id))
select count(distinct participant_id)
from mlegal where arrest_date < {self.q_t2}'''
        if pending is True:
            query = self.query_modify(str(query),f'''and (case_status_current regexp "diversion.*|.*pending" or case_outcome_date > {self.q_t1})''')
        df = self.query_run(query)
        return df
    
    def legal_in_custody(self, age = 19, cutoff_date = 'active_date', tally = True):
        '''
        Returns a table of clients in custody (or more realistically, whose last trunc_legal update was that they were in custody)

        Parameters:
            age: the cutoff age of clients to search for, defaults to 19
            cutoff date: the date on which to calculate clients ages, defaults to 'active_date' but one can also enter "t1", "t2" or a date formatted as "YYYY-MM-DD"
            tally(Bool): True returns a count of clients, False offers a table. defaults to True

        
        SQL Equivalent:

            with custody_ids as 
            (select distinct participant_id from 
                (select distinct participant_id from stints.classify_by_program
                join civicore.participants using(participant_ID)
                where active_date > DATE_ADD(birth_date, INTERVAL 19 YEAR)) l
            join civicore.trunc_legal tl using(participant_id)
            )
            select custody_status, count(distinct participant_id) from 
            (select x.participant_id, x.recent_date, 
                group_concat(distinct legal_status  SEPARATOR ', ') as legal_status, 
                group_concat(distinct legal_status_detail SEPARATOR ', ') as legal_status_detail, 
                group_concat(distinct custody_status  SEPARATOR ', ') custody_status, 
                group_concat(distinct comments SEPARATOR ', ') comments 
            from 
                (select participant_id, max(trunc_date) as recent_date from custody_ids
                left join civicore.trunc_legal using(participant_id)
                group by participant_id) x
            join civicore.trunc_legal l on x.participant_id = l.participant_id and x.recent_date = l.trunc_date
            group by participant_id) y
            group by custody_status;
        '''
        if cutoff_date.lower() == "t1":
            cutoff_date = self.t1
        if cutoff_date.lower()  == "t2":
            cutoff_date = self.t2
        if cutoff_date.lower() != "active_date":
            cutoff_date = f'"{cutoff_date}"'
        if tally is True: 
            query = f'''with custody_ids as (select distinct participant_id from (select distinct participant_id from {self.table}
    join civicore.participants using(participant_ID)
    where {cutoff_date} > DATE_ADD(birth_date, INTERVAL {age} YEAR)) l
    join civicore.trunc_legal tl using(participant_id)
    )

select custody_status, count(distinct participant_id) from (select x.participant_id, x.recent_date, group_concat(distinct legal_status  SEPARATOR ', ') as legal_status, group_concat(distinct legal_status_detail SEPARATOR ', ') as legal_status_detail, group_concat(distinct custody_status  SEPARATOR ', ') custody_status, group_concat(distinct comments SEPARATOR ', ') comments from (select participant_id, max(trunc_date) as recent_date from custody_ids
    left join civicore.trunc_legal using(participant_id)
    group by participant_id) x
    join civicore.trunc_legal l on x.participant_id = l.participant_id and x.recent_date = l.trunc_date
    group by participant_id) y
    group by custody_status'''
        else:
            query = f'''with custody_ids as (select distinct participant_id from (select distinct participant_id from {self.table}
    join civicore.participants using(participant_ID)
    where {cutoff_date} > DATE_ADD(birth_date, INTERVAL {age} YEAR)) l
    join civicore.trunc_legal tl using(participant_id)
    )

    select x.participant_id, x.recent_date, group_concat(distinct legal_status  SEPARATOR ', '), group_concat(distinct legal_status_detail SEPARATOR ', '), group_concat(distinct custody_status  SEPARATOR ', '), group_concat(distinct comments SEPARATOR ', ') from (select participant_id, max(trunc_date) as recent_date from custody_ids
    left join civicore.trunc_legal using(participant_id)
    group by participant_id) x
    join civicore.trunc_legal l on x.participant_id = l.participant_id and x.recent_date = l.trunc_date
    group by participant_id;'''
        df = self.query_run(query)
        return df
    
    def legal_program_involvement(self, program_timeframe = False):
        '''
        returns linkage and case session tallies for clients with cases ended in a timeframe

        Parameters:
            program_timeframe: True only counts linkages/case sessions in timeframe, defaults to False
        
        
        SQL Equivalent:

            with outcomes as (select participant_id, charge, case_type,
                case when case_outcome like "plead%" then concat(case_outcome, " - ", concat_sentence) else case_outcome end as case_outcome from 
                (select * from 
                    (SELECT participant_id, case_outcome, GROUP_CONCAT(distinct class_prior_to_plea_trial separator ", ") as charge, 
                    GROUP_CONCAT(DISTINCT case_type separator ", ") as case_type, GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS concat_sentence, 
                    GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes
                    FROM (select participant_id, class_prior_to_plea_trial, case_type, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
                join stints.classify_by_program using(participant_id) where case_outcome_date BETWEEN '2023-08-01' AND '2023-08-31') r
                group by participant_id, case_outcome) d1
            left join tasks.highest_case using(case_outcome)) d),
            links as (select participant_id,
                count(case when linked_date between '2023-08-01' and '2023-08-31' then 1 else null end) as link_ct,
                count(case when start_date between '2023-08-01' and '2023-08-31' then 1 else null end) as start_ct
                from civicore.linkages
                where hub_initiated = "yes"
                group by participant_id),
            sessions as (select participant_id,
                COUNT(DISTINCT CASE WHEN (hours = 0 OR indirect_type = 'Unsuccessful Attempt') THEN case_session_id else null END) AS unsuccessful,
                COUNT(DISTINCT CASE WHEN hours > 0 AND (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') THEN case_session_id else null END) AS successful
            from stints.classify_by_program
            join civicore.case_sessions using(participant_id)
            where (session_date BETWEEN '2023-08-01' and '2023-08-31') and session_casenote = "Casenote"
            group by participant_id)
            select o.participant_id, charge, case_type, case_outcome, link_ct linkages_made, start_ct linkages_started, successful, unsuccessful from outcomes o
            left join links l on o.participant_id = l.participant_id
            left join sessions s on o.participant_id = s.participant_id;
        '''
        if program_timeframe is True: 
            query = f'''with outcomes as (select participant_id, charge, case_type,
        case when case_outcome like "plead%" then concat(case_outcome, " - ", concat_sentence) else case_outcome end as case_outcome from (select * from (SELECT participant_id, case_outcome, GROUP_CONCAT(distinct class_prior_to_plea_trial separator ", ") as charge, GROUP_CONCAT(DISTINCT case_type separator ", ") as case_type, GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS concat_sentence, GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes
        FROM (select participant_id, class_prior_to_plea_trial, case_type, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
        join {self.table} using(participant_id) where case_outcome_date BETWEEN {self.q_t1} AND {self.q_t2}) r
        group by participant_id, case_outcome) d1
        left join tasks.highest_case using(case_outcome)) d),
    links as (select participant_id,
    count(case when linked_date between {self.q_t1} and {self.q_t2} then 1 else null end) as link_ct,
    count(case when start_date between {self.q_t1} and {self.q_t2} then 1 else null end) as start_ct
    from civicore.linkages
    where hub_initiated = "yes"
    group by participant_id),
    sessions as (select participant_id,
            COUNT(DISTINCT CASE WHEN (hours = 0 OR indirect_type = 'Unsuccessful Attempt') THEN case_session_id else null END) AS unsuccessful,
        COUNT(DISTINCT CASE WHEN hours > 0 AND (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') THEN case_session_id else null END) AS successful
    from {self.table}
    join civicore.case_sessions using(participant_id)
    where (session_date BETWEEN {self.q_t1} and {self.q_t2}) and session_casenote = "Casenote"
    group by participant_id)

    select o.participant_id, charge, case_type, case_outcome, link_ct linkages_made, start_ct linkages_started, successful, unsuccessful from outcomes o
    left join links l on o.participant_id = l.participant_id
    left join sessions s on o.participant_id = s.participant_id'''
        else:
            query = f'''with outcomes as (select participant_id, charge, case_type,
        case when case_outcome like "plead%" then concat(case_outcome, " - ", concat_sentence) else case_outcome end as case_outcome from (select * from (SELECT participant_id, case_outcome, GROUP_CONCAT(distinct class_prior_to_plea_trial separator ", ") as charge, GROUP_CONCAT(DISTINCT case_type separator ", ") as case_type, GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS concat_sentence, GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes
        FROM (select participant_id, class_prior_to_plea_trial, case_type, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
        join {self.table} using(participant_id) where case_outcome_date BETWEEN {self.q_t1} AND {self.q_t2}) r
        group by participant_id, case_outcome) d1
        left join tasks.highest_case using(case_outcome)) d),
    links as (select participant_id,
    count(case when linked_date is not null then 1 else null end) as link_ct,
    count(case when start_date is not null then 1 else null end) as start_ct
    from civicore.linkages
    where hub_initiated = "yes"
    group by participant_id),
    sessions as (select participant_id,
            COUNT(DISTINCT CASE WHEN (hours = 0 OR indirect_type = 'Unsuccessful Attempt') THEN case_session_id else null END) AS unsuccessful,
        COUNT(DISTINCT CASE WHEN hours > 0 AND (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') THEN case_session_id else null END) AS successful
    from {self.table}
    join civicore.case_sessions using(participant_id)
    where (session_date is not null) and session_casenote = "Casenote"
    group by participant_id)

    select o.participant_id, charge, case_type, case_outcome, link_ct linkages_made, start_ct linkages_started, successful, unsuccessful from outcomes o
    left join links l on o.participant_id = l.participant_id
    left join sessions s on o.participant_id = s.participant_id'''
        df = self.query_run(query)
        df = df.fillna(0)
        df['order'] = df.groupby('participant_id').cumcount() + 1
        pivoted_df = df.pivot_table(index=['participant_id', 'linkages_made', 'linkages_started', 'successful', 'unsuccessful'], columns=['order'], values=['charge', 'case_type', 'case_outcome'], aggfunc='first')
        pivoted_df.columns = [f'{name}_{i}' for name, i in zip(pivoted_df.columns.get_level_values(0), pivoted_df.columns.get_level_values(1))]
        max_cases = df['order'].max()
        
        case_columns = [f'charge_{i}' for i in range(1, max_cases + 1)] + [f'case_type_{i}' for i in range(1, max_cases + 1)] + [f'case_outcome_{i}' for i in range(1, max_cases + 1)]
        case_columns = [item for sublist in zip(case_columns[:max_cases], case_columns[max_cases*1:max_cases*2], case_columns[max_cases*2:]) for item in sublist]
        other_columns = [col for col in pivoted_df.columns if col not in case_columns]
        pivoted_df = pivoted_df[case_columns + other_columns]
        pivoted_df = pivoted_df.reset_index()
        if self.clipboard is True:
            pivoted_df.to_clipboard()
        return pivoted_df

    def legal_fel_reduction(self, timeframe = True):
        '''
        Counts the number of clients who had their felony classes reduced after trial. Requires a felony_classes table

        Parameters:
            timeframe(Bool): if true, only looks at cases between T1 and T2. Defaults to True
        
        
        SQL Equivalent:

            select felony, count(felony) FROM 
                (select CASE 
                    WHEN concatenated_values LIKE "%Decreased%" THEN 'lowered' 
                    ELSE 'remained' 
                END AS felony from
                    (SELECT participant_id, GROUP_CONCAT(rank_comparison SEPARATOR ', ') AS concatenated_values 
                        FROM (select lt.participant_id, lt.class_prior_to_plea_trial,  lt.class_after_plea_trial,  
                            CASE
                                WHEN crr1.ranking < crr2.ranking THEN 'Decreased'
                                ELSE 'Not Lower'
                            END AS rank_comparison from civicore.legal lt 
                            join stints.classify_by_program using(participant_id) 
                            JOIN tasks.felony_classes crr1 ON lt.class_prior_to_plea_trial = crr1.class 
                            JOIN tasks.felony_classes crr2 ON lt.class_after_plea_trial = crr2.class 
                            where class_after_plea_trial IS NOT NULL AND case_outcome_date BETWEEN '2023-08-01' and '2023-08-31') h 
                        GROUP BY participant_id) j
                    ) k 
                group by felony;
        '''
        if timeframe is True: 
            query = f'''select felony, count(felony) FROM 
    (select CASE 
                    WHEN concatenated_values LIKE "%Decreased%" THEN 'lowered' 
            ELSE 'remained' END AS felony  
            from(SELECT participant_id, GROUP_CONCAT(rank_comparison SEPARATOR ', ') AS concatenated_values 
            FROM (select lt.participant_id, lt.class_prior_to_plea_trial,  lt.class_after_plea_trial,  
            CASE         
                            WHEN crr1.ranking < crr2.ranking THEN 'Decreased'         
                            ELSE 'Not Lower'     
                END AS rank_comparison from civicore.legal lt 
                    join {self.table} using(participant_id) 
            JOIN tasks.felony_classes crr1 ON lt.class_prior_to_plea_trial = crr1.class 
            JOIN tasks.felony_classes crr2 ON lt.class_after_plea_trial = crr2.class 
            where class_after_plea_trial IS NOT NULL AND case_outcome_date BETWEEN {self.q_t1} and {self.q_t2}) h 
            GROUP BY participant_id) j) k group by felony;'''
        else:
            query = f'''select felony, count(felony) FROM 
    (select CASE 
                    WHEN concatenated_values LIKE "%Decreased%" THEN 'lowered' 
            ELSE 'remained' END AS felony  
            from(SELECT participant_id, GROUP_CONCAT(rank_comparison SEPARATOR ', ') AS concatenated_values 
            FROM (select lt.participant_id, lt.class_prior_to_plea_trial,  lt.class_after_plea_trial,  
            CASE         
                            WHEN crr1.ranking < crr2.ranking THEN 'Decreased'         
                            ELSE 'Not Lower'     
                END AS rank_comparison from civicore.legal lt 
                    join {self.table} using(participant_id) 
            JOIN tasks.felony_classes crr1 ON lt.class_prior_to_plea_trial = crr1.class 
            JOIN tasks.felony_classes crr2 ON lt.class_after_plea_trial = crr2.class 
            where class_after_plea_trial IS NOT NULL) h 
            GROUP BY participant_id) j) k group by felony;'''
        df = self.query_run(query)
        return df
    
    def legal_case_outcomes(self,  timeframe = True):
        '''
        returns a count of client legal outcomes, keeping the most severe for each client (ie: if a client gets probation and IDOC, only IDOC is counted). requires tasks.highest_case (ELI FIND THIS TOO)

        Parameters:
            timeframe (Bool): if true, only looks at cases between T1 and T2. Defaults to True

        
        SQL Equivalent:

            with ranked_df as 
            (select * from 
                (SELECT participant_id, case_outcome, 
                GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS sentence, 
                GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes FROM 
                    (select participant_id, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
                    join stints.classify_by_program using(participant_id) where case_outcome_date BETWEEN '2023-08-01' AND '2023-08-31') r
                group by participant_id, case_outcome) d1
            left join tasks.highest_case using(case_outcome)),
            plea_concat as 
                (select case when case_outcome like "plead guilty" then concat(case_outcome, " - ", sentence) 
                else case_outcome end as case_outcome
            from (select case_outcome, sentence from ranked_df d1
            LEFT JOIN (select participant_id, ranking from ranked_df) d2 ON d1.participant_id = d2.participant_id AND d1.ranking < d2.ranking
            WHERE d2.ranking IS NULL)h)
            select case_outcome, count(case_outcome) from plea_concat
            group by case_outcome
        '''
        if timeframe is True: 
            query = f'''with ranked_df as (select * from (SELECT participant_id, case_outcome, GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS sentence, GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes
    FROM (select participant_id, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
    join {self.table} using(participant_id) where case_outcome_date BETWEEN {self.q_t1} AND {self.q_t2}) r
    group by participant_id, case_outcome) d1
    left join tasks.highest_case using(case_outcome)), 
    plea_concat as (select case when case_outcome like "plead guilty" then concat(case_outcome, " - ", sentence) else case_outcome end as case_outcome
    from (select case_outcome, sentence from ranked_df d1
    LEFT JOIN (select participant_id, ranking from ranked_df) d2 ON d1.participant_id = d2.participant_id AND d1.ranking < d2.ranking
    WHERE
        d2.ranking IS NULL)h)
    select case_outcome, count(case_outcome) from plea_concat
    group by case_outcome
        '''
        else: 
            query = f'''with ranked_df as (select * from (SELECT participant_id, case_outcome, GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS sentence, GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes
    FROM (select participant_id, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
    join {self.table} using(participant_id)) r
    group by participant_id, case_outcome) d1
    left join tasks.highest_case using(case_outcome)), 
    plea_concat as (select case when case_outcome like "plead%" then concat(case_outcome, " - ", sentence) else case_outcome end as case_outcome
    from (select case_outcome, sentence from ranked_df d1
    LEFT JOIN (select participant_id, ranking from ranked_df) d2 ON d1.participant_id = d2.participant_id AND d1.ranking < d2.ranking
    WHERE
        d2.ranking IS NULL)h)
    select case_outcome, count(case_outcome) from plea_concat
    group by case_outcome'''
        df = self.query_run(query)
        return df    
    
    def legal_case_statuses(self, timeframe = False, opened_closed = "closed"):
        '''
        returns a count of client case statuses at a given time, or the count of open/closed cases between t1 and t2

        Parameters:
            timeframe(Bool): if true, only looks at cases between T1 and T2. Defaults to False
            opened_closed: only needed if timeframe is True. "closed" returns a count of clients whose cases closed in the timeframe, "opened" returns cases opened
        
        
        SQL Equivalent:

            with mlegal as (select * from (select participant_id, active_date from stints.classify_by_program) m
            join civicore.legal l using(participant_id))
            select case_status_current,
                COUNT(DISTINCT CASE WHEN charge LIKE "%Felony%" THEN case_id END) AS Felony,
                COUNT(DISTINCT CASE WHEN charge LIKE "%Misdemeanor%" THEN case_id END) AS Misdemeanor
            from mlegal
            WHERE active_date <= DATE_ADD(arrest_date, INTERVAL 60 DAY) AND case_status_current regexp "Case Pending|Case Closed|Probation"
            group by case_status_current
            UNION ALL
            select case_outcome,
                COUNT(DISTINCT CASE WHEN charge LIKE "%Felony%" THEN case_id END) AS Felony,
                COUNT(DISTINCT CASE WHEN charge LIKE "%Misdemeanor%" THEN case_id END) AS Misdemeanor
            from mlegal n
            WHERE active_date <= DATE_ADD(arrest_date, INTERVAL 60 DAY) AND case_outcome IS NOT NULL
            group by case_outcome
            order by case when case_status_current = 'Case Pending' then 1
                        when case_status_current = 'Case Closed' then 2
                        when case_status_current = 'Dismissed' then 3
                        when case_status_current = 'Plead Guilty' then 4
                        when case_status_current = 'Found Guilty' then 5
                        else 6
                        end asc;
            
        '''
        if timeframe is False:
            query = f'''with mlegal as (select * from (select participant_id, active_date from {self.table}) m
join civicore.legal l using(participant_id))
select case_status_current,
COUNT(DISTINCT CASE WHEN charge LIKE "%Felony%" THEN case_id END) AS Felony,
COUNT(DISTINCT CASE WHEN charge LIKE "%Misdemeanor%" THEN case_id END) AS Misdemeanor
from mlegal
WHERE active_date <= DATE_ADD(arrest_date, INTERVAL 60 DAY) AND case_status_current regexp "Case Pending|Case Closed|Probation"
group by case_status_current
UNION ALL 
select case_outcome,
COUNT(DISTINCT CASE WHEN charge LIKE "%Felony%" THEN case_id END) AS Felony,
COUNT(DISTINCT CASE WHEN charge LIKE "%Misdemeanor%" THEN case_id END) AS Misdemeanor
from mlegal n
WHERE active_date <= DATE_ADD(arrest_date, INTERVAL 60 DAY) AND case_outcome IS NOT NULL
group by case_outcome
order by case when case_status_current = 'Case Pending' then 1
            when case_status_current = 'Case Closed' then 2
            when case_status_current = 'Dismissed' then 3
            when case_status_current = 'Plead Guilty' then 4
            when case_status_current = 'Found Guilty' then 5
            else 6
            end asc'''
        if timeframe is True:
            if opened_closed.lower() == "opened" or opened_closed.lower() == "open":
                relevant_date = "arrest_date"
            else:
                relevant_date = "case_outcome_date"
            query = f'''with mlegal as (select * from (select participant_id, active_date from {self.table}) m
join civicore.legal l using(participant_id))
select case_status_current,
COUNT(DISTINCT CASE WHEN charge LIKE "%Felony%" THEN case_id END) AS Felony,
COUNT(DISTINCT CASE WHEN charge LIKE "%Misdemeanor%" THEN case_id END) AS Misdemeanor
from mlegal
WHERE active_date <= DATE_ADD(arrest_date, INTERVAL 45 DAY) AND case_status_current regexp "Case Pending|Case Closed|Probation" and ({relevant_date} between {self.q_t1} and {self.q_t2})
group by case_status_current
UNION ALL 
select case_outcome,
COUNT(DISTINCT CASE WHEN charge LIKE "%Felony%" THEN case_id END) AS Felony,
COUNT(DISTINCT CASE WHEN charge LIKE "%Misdemeanor%" THEN case_id END) AS Misdemeanor
from mlegal n
WHERE active_date <= DATE_ADD(arrest_date, INTERVAL 30 DAY) AND case_outcome IS NOT NULL and ({relevant_date} between {self.q_t1} and {self.q_t2})
group by case_outcome
order by case when case_status_current = 'Case Pending' then 1
when case_status_current = 'Case Closed' then 2
when case_status_current = 'Dismissed' then 3
when case_status_current = 'Plead Guilty' then 4
when case_status_current = 'Found Guilty' then 5
else 6
end asc'''
        df = self.query_run(query)
        return df
    
    def legal_one_case(self, timeframe = True):
        '''
        Returns counts of case outcomes for clients with one case

        Parameters:
            timeframe(Bool): if true, only looks at cases between T1 and T2. Defaults to True
        
        
        SQL Equivalent:

            with one_cases as (select * from 
                (select participant_id from 
                    (select participant_id, count(distinct participant_id) as case_count from stints.classify_by_program
                    join civicore.legal l using(participant_id)
                    where case_outcome_date BETWEEN '2023-08-01' AND '2023-08-31'
                    group by participant_id) d
                where case_count = 1) e
            join civicore.legal using(participant_id)
            where case_outcome_date BETWEEN '2023-08-01' AND '2023-08-31')
            select case_outcome, count(case_outcome) from one_cases
            group by case_outcome;
        '''       
        if timeframe is True:
            query = f'''with one_cases as (select * from (select participant_id from (select participant_id, count(distinct participant_id) as case_count from {self.table}
join civicore.legal l using(participant_id)
where case_outcome_date BETWEEN {self.q_t1} AND {self.q_t2}
group by participant_id) d
where case_count = 1) e
join civicore.legal using(participant_id)
where case_outcome_date BETWEEN {self.q_t1} AND {self.q_t2})

select case_outcome, count(case_outcome) from one_cases
group by case_outcome'''
        else:
            query = f'''with one_cases as (select * from (select participant_id from (select participant_id, count(distinct participant_id) as case_count from {self.table}
join civicore.legal l using(participant_id)
group by participant_id) d
where case_count = 1) e
join civicore.legal using(participant_id))

select case_outcome, count(case_outcome) from one_cases
group by case_outcome'''
        df = self.query_run(query)
        return df
    
    def legal_case_type(self, timeframe = False, first_charge = False, opened_closed = "opened", percentage = False, case_client = "case"):
        '''
        Returns counts of cases by type (gun, drug, etc) 

        Parameters:
            timeframe (Bool): if true, only looks at cases between T1 and T2. Defaults to False
            first_charge(Bool): if true, only returns earliest case(s) in CiviCore. Defaults to False
            opened_closed: only needed if timeframe is True. "closed" returns a count of clients whose cases closed in the timeframe, "opened" returns cases opened
            percentage (Bool): if True, returns a percentage breakdown. Defaults to False
            case_client: "case" counts number of cases, "client" counts number of clients. Defaults to "case"
        
        
        SQL Equivalent:

            WITH mleg AS (
                SELECT *
                FROM (
                    SELECT DISTINCT participant_id, active_date, closed_date
                    FROM participants.hd
                ) k
                JOIN civicore.legal l USING (participant_id)
                JOIN tasks.last_close USING (participant_id)
                WHERE last_closed_date IS NULL OR arrest_date > last_closed_date
            ),
                mlegal AS (
                SELECT m1.*
                FROM mleg m1
                JOIN (
                    SELECT participant_id, MIN(arrest_date) AS min_date
                    FROM mleg
                    GROUP BY participant_id
                ) m2 ON m1.participant_id = m2.participant_id AND m1.arrest_date = m2.min_date
            )
            select case_type,
            COUNT(DISTINCT CASE WHEN charge regexp "Felony.*" THEN case_id END) AS Felony,
            COUNT(DISTINCT CASE WHEN charge regexp "Misdemeanor.*" THEN case_id END) AS Misdemeanor
            from mlegal
            group by case_type
            ORDER BY case when case_type = 'property' then 1
            when case_type = 'gun' then 2
            when case_type = 'battery' then 3
            when case_type = 'drug' then 4
            when case_type = 'attempted murder' then 5
            when case_type = 'murder' then 6
            when case_type = 'other' then 7
            else 8
            end asc
        '''
        if case_client == "case":
            case_client = "case_id"
        if case_client =="client":
            case_client = "participant_id"     
        if first_charge is True:
            query = f'''
    WITH mleg AS (SELECT *
    FROM (
        SELECT DISTINCT participant_id, active_date, closed_date
        FROM {self.table}) k
    JOIN civicore.legal l USING (participant_id)
    JOIN tasks.last_close USING (participant_id)
    WHERE last_closed_date IS NULL OR arrest_date > last_closed_date
),
    mlegal AS (
    SELECT m1.* FROM mleg m1
    JOIN (
        SELECT participant_id, MIN(arrest_date) AS min_date
        FROM mleg
        GROUP BY participant_id
    ) m2 ON m1.participant_id = m2.participant_id AND m1.arrest_date = m2.min_date
)
select case_type,
COUNT(DISTINCT CASE WHEN charge regexp "Felony.*" THEN case_id END) AS Felony,
COUNT(DISTINCT CASE WHEN charge regexp "Misdemeanor.*" THEN case_id END) AS Misdemeanor
from mlegal
group by case_type
ORDER BY case when case_type = 'property' then 1
when case_type = 'gun' then 2
when case_type = 'battery' then 3
when case_type = 'drug' then 4
when case_type = 'attempted murder' then 5
when case_type = 'murder' then 6
when case_type = 'other' then 7
else 8
end asc
'''
        else: 
            query = f'''
        WITH mlegal AS (SELECT *
        FROM (
            SELECT DISTINCT participant_id, active_date, closed_date
            FROM {self.table}
        ) k
        JOIN civicore.legal l USING (participant_id)
        JOIN tasks.last_close USING (participant_id)
        WHERE last_closed_date IS NULL OR arrest_date > last_closed_date
    )
    select case_type,
    COUNT(DISTINCT CASE WHEN charge regexp "Felony.*" THEN case_id END) AS Felony,
    COUNT(DISTINCT CASE WHEN charge regexp "Misdemeanor.*" THEN case_id END) AS Misdemeanor
    from mlegal
    group by case_type
    ORDER BY case when case_type = 'property' then 1
    when case_type = 'gun' then 2
    when case_type = 'battery' then 3
    when case_type = 'drug' then 4
    when case_type = 'attempted murder' then 5
    when case_type = 'murder' then 6
    when case_type = 'other' then 7
    else 8
    end asc
    '''
            if timeframe is True:
                if opened_closed.lower() == "opened" or opened_closed.lower() == "open":
                    opened_closed = "arrest_date"
                    query = self.query_modify(str(query), f'''where (({opened_closed} BETWEEN {self.q_t1} AND {self.q_t2}) or (active_date BETWEEN {self.q_t1} AND {self.q_t2}))''')
                else:
                    opened_closed = "case_outcome_date"
                    query = self.query_modify(str(query), f'''and(({opened_closed} BETWEEN {self.q_t1} AND {self.q_t2}))''')     
        df = self.query_run(query)
        if percentage is True:
            df = self.percentage_convert(df, replace= False)
        return df    
    
    def legal_in_custody_links(self):
        '''
        Returns the number of people in custody with linkages


        SQL Equivalent:

            with recent_custody as(select x.participant_id, l.custody_status from  (select participant_id, active_date, max(trunc_date) as recent_date from stints.classify_by_program c
            left join civicore.trunc_legal tl using(participant_id)
            group by participant_id, active_date) x
            join civicore.trunc_legal l on x.participant_id = l.participant_id and x.recent_date = l.trunc_date
            where active_date <= DATE_ADD(trunc_date, INTERVAL 45 DAY) and custody_status = 'In Custody (+)')

            select count(distinct participant_id) from recent_custody
            join civicore.linkages using(participant_id);
        '''
        query = f'''with recent_custody as(select x.participant_id, l.custody_status from  (select participant_id, active_date, max(trunc_date) as recent_date from {self.table} c
left join civicore.trunc_legal tl using(participant_id)
group by participant_id, active_date) x
join civicore.trunc_legal l on x.participant_id = l.participant_id and x.recent_date = l.trunc_date
where active_date <= DATE_ADD(trunc_date, INTERVAL 45 DAY) and custody_status = 'In Custody (+)')

select count(distinct participant_id) from recent_custody
join civicore.linkages using(participant_id)'''
        df = self.query_run(query)
        return df
    

    def legal_pending_cases(self, t1_t2 = "t1", also_ended = False):
        '''
        Counts the number of people with pending cases at a given point

        Parameters:
            t1_t2: the date on which to tally. "t1" uses the start of the stint, "t2" uses the end
            also_ended(Bool): True only counts clients who also had a case end. Defaults to False  
        
        
        SQL Equivalent:

            select count(distinct participant_id) from stints.classify_by_program
                join civicore.legal using(participant_id)
                where arrest_date < '2023-08-01' and (case_status_current like "Case Pending" or case_outcome_date > '2023-08-01');
        '''
        if t1_t2 == "t1": 
            t1_t2 = self.q_t1
        else:
            t1_t2 = self.q_t2
        if also_ended == False:
            query = f'''select count(distinct participant_id) from {self.table}
    join civicore.legal using(participant_id)
    where arrest_date < {t1_t2} and (case_status_current like "Case Pending" or case_outcome_date > {t1_t2})'''
            df = self.query_run(query)
            return df
        else:
            query = f'''with ranked_df as (select * from (SELECT participant_id, case_outcome, GROUP_CONCAT( DISTINCT sentence SEPARATOR ', ') AS concat_sentence, GROUP_CONCAT(conditions_notes SEPARATOR ', ') AS concat_notes
FROM (select  participant_id, conditions_notes, case_id, case_status_current, case_outcome, case_outcome_date, sentence from civicore.legal
join {self.table} using(participant_id) where case_outcome_date BETWEEN {self.q_t1} AND {self.q_t2}) r
group by participant_id, case_outcome) d1),

pending as (select participant_id from {self.table}
join civicore.legal using(participant_id)
where arrest_date < {t1_t2} and (case_status_current like "Case Pending" or case_outcome_date > {t1_t2}))

select count(distinct participant_id)
from ranked_df
join pending using(participant_id)'''
        df = self.query_run(query)
        return df

    def legal_rearrested(self, timeframe = True):
        '''
        Returns a count of clients rearrested

        Parameters:
            timeframe (Bool): True sums rearrests in the timeframe, False does not. Defaults to True

        SQL Equivalent:

            WITH mlegal AS (SELECT * FROM (
                    SELECT DISTINCT participant_id, active_date, closed_date
                    FROM stints.classify_by_program) k
                JOIN civicore.legal l USING (participant_id)
                JOIN tasks.last_close USING (participant_id)
                WHERE last_closed_date IS NULL OR arrest_date > last_closed_date)
            select count(distinct participant_id) from (select m1.participant_id, active_date, case_id, arrest_date, offense_date
            from mlegal m1
            join (select participant_id, min(arrest_date) as min_date from mlegal group by participant_id) m2 
            on m1.participant_id = m2.participant_id and m1.arrest_date > m2.min_date) s;
        '''
        query = f'''WITH mlegal AS (SELECT * FROM (
        SELECT DISTINCT participant_id, active_date, closed_date
        FROM {self.table}
    ) k
    JOIN civicore.legal l USING (participant_id)
    JOIN tasks.last_close USING (participant_id)
    WHERE last_closed_date IS NULL OR arrest_date > last_closed_date)
select count(distinct participant_id) from (select m1.participant_id, active_date, case_id, arrest_date, offense_date
from mlegal m1
join (select participant_id, min(arrest_date) as min_date from mlegal group by participant_id) m2 on m1.participant_id = m2.participant_id and m1.arrest_date > m2.min_date) s'''
        if timeframe is True:
            query = f'''WITH mlegal AS (SELECT * FROM (
        SELECT DISTINCT participant_id, active_date, closed_date
        FROM {self.table}
    ) k
    JOIN civicore.legal l USING (participant_id)
    JOIN tasks.last_close USING (participant_id)
    WHERE last_closed_date IS NULL OR arrest_date > last_closed_date)
select count(distinct participant_id) from (select m1.participant_id, active_date, case_id, arrest_date, offense_date
from mlegal m1
join (select participant_id, min(arrest_date) as min_date from mlegal group by participant_id) m2 on m1.participant_id = m2.participant_id and m1.arrest_date > m2.min_date) s
where arrest_date between {self.q_t1} and {self.q_t2}''' 
        df = self.query_run(query)
        return df

    def link_tally(self, hub_initiated = True, timeframe = False, started_linked = "linked_date"):
        '''
        Returns the number of clients with at least one linkage

        Parameters:
            hub_initiated (Bool): whether to exclusively count hub_initiated linkages. Note that "False" returns a count of both hub initiated and non hub initiated linkages. Defaults to True
            timeframe (Bool): whether to only count people who had at least one linkage between T1 and T2, defaults to False
            started_linked: only needed if timeframe is True. "linked_date" counts linkages initiated , "start_date" counts those that started, and "both" counts both cases. Defaults to "linked_date"

        
        SQL Equivalent:

            select count(distinct participant_id) from 
                (select participant_id from stints.classify_by_program) x 
            join civicore.linkages using (participant_id) where hub_initiated = "yes";
        '''
        query = f'''select count(distinct participant_id) from (select participant_id from {self.table}) x join civicore.linkages using (participant_id)'''
        if hub_initiated is False and timeframe is False:
            df = self.query_run(query)
            return df
        if started_linked.lower() == "both":
            if hub_initiated is False:
                query = self.query_modify(str(query), f'''where (linked_date between {self.q_t1} and {self.q_t2}) or (start_date between {self.q_t1} and {self.q_t2})''')
            if hub_initiated is True:
                query = self.query_modify(str(query), f'''where hub_initiated = "yes" and ((linked_date between {self.q_t1} and {self.q_t2}) or (start_date between {self.q_t1} and {self.q_t2}))''')
        if hub_initiated is True and timeframe is False:
            query = self.query_modify(str(query), f'''where hub_initiated = "yes"''')
        else:
            if hub_initiated is False and timeframe is True:
                query = self.query_modify(str(query), f'''where {started_linked} between {self.q_t1} and {self.q_t2}''')
            if hub_initiated is True and timeframe is True:
                query = self.query_modify(str(query), f'''where hub_initiated = "yes" and ({started_linked} between {self.q_t1} and {self.q_t2})''')
        df = self.query_run(query)
        return df
    
    def link_edu_job(self, hub_initiated = True, link_type = "education", age = 19, cutoff_date = 't1', first_n_months = None):
        '''
        Counts either meployment or education linkages among clients

        Parameters:
            hub_initiated (Bool): whether to exclusively count hub_initiated linkages. Note that "False" returns a count of both hub initiated and non hub initiated linkages. Defaults to True
            link_type: "education" returns education linkages, while "employment" returns employment linkages
            age: the age cutoff for clients, defaults to 19
            cutoff date: the date on which to calculate clients ages, defaults to 't1' but one can also enter "active_date", "t2" or a date formatted as "YYYY-MM-DD"
            first_n_months(integer): optional, only counts linkages made in the first N months of a client's stint
        
        
        SQL Equivalent:

            select count(distinct participant_id) from civicore.linkages l
            join 
                (select participant_id, birth_date, active_date, "2023-08-01" from stints.classify_by_program
                join civicore.participants using(participant_ID)
                where "2023-08-01" < DATE_ADD(birth_date, INTERVAL 19 YEAR)) fccc using (participant_id)
            join(civicore.participants p) using(participant_id)
            where l.linkage_type = 'education' AND linked_date > active_date and hub_initiated = "yes";
        '''
        if cutoff_date == "t1":
            cutoff_date = self.t1
        if link_type.lower() == "education":
            inequality_sign = '<'
        if link_type.lower() == "employment":
            inequality_sign = '>'
        link_type = f"'{link_type}'"
        if cutoff_date.lower() != "active_date":
            cutoff_date = f'"{cutoff_date}"'
        query = f'''select count(distinct participant_id) from civicore.linkages l
join (select participant_id, birth_date, active_date, {cutoff_date} from {self.table}
join civicore.participants using(participant_ID)
where {cutoff_date} {inequality_sign} DATE_ADD(birth_date, INTERVAL {age} YEAR)) fccc using (participant_id)
join(civicore.participants p) using(participant_id)
where l.linkage_type = {link_type} AND linked_date > active_date'''
        if hub_initiated == True:
            query = self.query_modify(str(query), f'''and hub_initiated = "yes"''')
        if first_n_months is not None:
            query = self.query_modify(str(query), f'''AND DATEDIFF(active_date, l.linked_date) <= {first_n_months} * 30.5''')
        df = self.query_run(query)
        return df

    def link_goal_area(self, hub_initiated = True, timeframe = True, started_linked = "linked_date"):
        '''
        Returns counts of linkages by goal area (service).

        Parameters:
            hub_initiated (Bool): whether to exclusively count hub_initiated linkages. Note that "False" returns a count of both hub initiated and non hub initiated linkages. Defaults to True
            timeframe (Bool): whether to only count linkages made between T1 and T2, defaults to True
            started_linked: only needed if timeframe is True. "linked_date" counts linkages initiated , "start_date" counts those that started, and "both" counts both cases. Defaults to "linked_date"
        
        
        SQL Equivalent:

            with separated_goals as 
                (SELECT participant_id, first_name, last_name, linkage_type, linkage_org, linked_date, s.start_date, hub_initiated, 
                SUBSTRING_INDEX(SUBSTRING_INDEX(goal_area, ', ', n), ', ', -1) AS separated_area FROM 
                    (select participant_id, first_name, last_name, linkage_type, linkage_org, goal_area, linked_date, l.start_date, hub_initiated from stints.classify_by_program p
                    join civicore.linkages l using(participant_id)) s
                JOIN (
                    SELECT 1 AS n UNION ALL
                    SELECT 2 UNION ALL
                    SELECT 3 UNION ALL
                    SELECT 4 UNION ALL
                    SELECT 5 UNION ALL
                    SELECT 6 UNION ALL
                    SELECT 7 UNION ALL
                    SELECT 8 UNION ALL
                    SELECT 9
                ) AS numbers
                ON CHAR_LENGTH(goal_area) - CHAR_LENGTH(REPLACE(goal_area, ',', '')) >= n - 1)

            select separated_area, count(distinct participant_id)
            from separated_goals
            where linked_date between '2023-08-01' and '2023-08-31' and hub_initiated = 'Yes' group by separated_area;
        '''
        
        query = f'''with separated_goals as (SELECT participant_id, first_name, last_name, linkage_type, linkage_org, linked_date, s.start_date, hub_initiated, SUBSTRING_INDEX(SUBSTRING_INDEX(goal_area, ', ', n), ', ', -1) AS separated_area
FROM (select participant_id, first_name, last_name, linkage_type, linkage_org, goal_area, linked_date, l.start_date, hub_initiated from {self.table} p
join civicore.linkages l using(participant_id)) s
JOIN (
    SELECT 1 AS n UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5 UNION ALL
    SELECT 6 UNION ALL
    SELECT 7 UNION ALL
    SELECT 8 UNION ALL
    SELECT 9
) AS numbers
ON CHAR_LENGTH(goal_area) - CHAR_LENGTH(REPLACE(goal_area, ',', '')) >= n - 1)

select separated_area, count(distinct participant_id)
from separated_goals
group by separated_area
'''
        if timeframe == True:
            if started_linked == "linked_date" or started_linked == "start_date":
                query = self.query_modify(str(query), f'''where {started_linked} between {self.q_t1} and {self.q_t2}''')
            if started_linked == "both" or started_linked == "Both":
                query = self.query_modify(str(query), f'''where (start_date between {self.q_t1} and {self.q_t2} or linked_date between {self.q_t1} and {self.q_t2})''')
            if hub_initiated == True:
                query = self.query_modify(str(query), "and hub_initiated = 'Yes'")
        if timeframe == False and hub_initiated == True:
            query = self.query_modify(str(query), "where hub_initiated = 'Yes'")
        df = self.query_run(query)
        return df
    
    def mediation_tally(self, followup = False):
        '''
        counts number of mediations in timeframe

        Parameters
            followup (Bool): if True, returns followup mediation tally. Defaults to False
        '''
        if followup == False:
            query = f'''select outcome_mediation, count(outcome_mediation) from civicore.mediations
where (date_start_mediation between{self.q_t1} and {self.q_t2})
group by outcome_mediation'''
        
        else:
            query = f'''select outcome_mediation, count(outcome_mediation) from civicore.mediation_followup
    where (date_session between{self.q_t1} and {self.q_t2})
    group by outcome_mediation'''
        df = self.query_run(query)
        return df
    
    def mediation_time_spent(self):
        '''
        sums the number of hours spent on mediations
        '''
        query = f'''select sum(hours_spent_mediation) from civicore.mediations where (date_conflict between {self.q_t1} and {self.q_t2})'''
        df = self.query_run(query)
        return df
    def programs_packages(self):
        '''
        Provides counts and percentages of different program combinations

        
        SQL Equivalent:

            WITH services_row as 
            (SELECT participant_id,
                GROUP_CONCAT(distinct CASE
                    WHEN service_legal = 1 THEN 'Legal' ELSE NULL
                    END) AS Legal,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN service_cm = 1 THEN 'Case Management' ELSE NULL
                    END) AS "CM",
                GROUP_CONCAT(DISTINCT CASE
                    WHEN cm_juv_divert = 1 THEN 'YIP' ELSE NULL
                    END) AS YIP,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN cm_scan = 1 THEN 'SCaN' ELSE NULL
                    END) AS SCaN,
                GROUP_CONCAT(DISTINCT CASE WHEN cm_scan_outreach = 1 THEN 'SCaN Outreach'
                    ELSE NULL
                    END) AS "SCaN_Outreach",
                GROUP_CONCAT( DISTINCT CASE
                    WHEN hd = 1 THEN 'HD'
                    ELSE NULL
                    END) AS HD,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN rjcc = 1 THEN 'RJCC'
                    ELSE NULL
                    END) AS RJCC,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN crws = 1 THEN 'CRwS'
                    ELSE NULL
                    END) AS CRwS,
                GROUP_CONCAT(distinct CASE
                        WHEN vp = 1 THEN 'VP'
                        ELSE NULL
                    END) AS VP
            FROM stints.classify_by_program
            WHERE service_legal = 1 OR service_cm = 1 OR cm_juv_divert = 1
                OR cm_scan = 1 OR cm_scan_outreach = 1 OR hd = 1
                OR rjcc = 1 OR crws = 1 OR vp = 1
            GROUP BY participant_id),
            concat_services as (SELECT
                participant_id,
                CONCAT_WS(' + ', Legal, CM, YIP, SCaN, SCaN_Outreach, HD, RJCC, CRwS, VP
                ) AS concatenated_services
            FROM (services_row))
            select concatenated_services, count(concatenated_services), 
            count(concatenated_services)/(SELECT COUNT(*) FROM concat_services) as percentage
            from concat_services
            group by concatenated_services
            order by count(concatenated_services) desc;

        '''
        query = f'''WITH services_row as 
(SELECT
    participant_id,
    GROUP_CONCAT(distinct
        CASE
            WHEN service_legal = 1 THEN 'Legal'
            ELSE NULL
        END
    ) AS Legal,
    GROUP_CONCAT(DISTINCT
        CASE
            WHEN service_cm = 1 THEN 'Case Management'
            ELSE NULL
        END
    ) AS "CM",
    GROUP_CONCAT(DISTINCT
        CASE
            WHEN cm_juv_divert = 1 THEN 'YIP'
            ELSE NULL
        END
    ) AS YIP,
    GROUP_CONCAT(DISTINCT
        CASE
            WHEN cm_scan = 1 THEN 'SCaN'
            ELSE NULL
        END
    ) AS SCaN,
    GROUP_CONCAT(DISTINCT
        CASE
            WHEN cm_scan_outreach = 1 THEN 'SCaN Outreach'
            ELSE NULL
        END
    ) AS "SCaN_Outreach",
    GROUP_CONCAT( DISTINCT
        CASE
            WHEN hd = 1 THEN 'HD'
            ELSE NULL
        END
    ) AS HD,
    GROUP_CONCAT(DISTINCT
        CASE
            WHEN rjcc = 1 THEN 'RJCC'
            ELSE NULL
        END
    ) AS RJCC,
    GROUP_CONCAT(DISTINCT
        CASE
            WHEN crws = 1 THEN 'CRwS'
            ELSE NULL
        END
    ) AS CRwS,
    GROUP_CONCAT(distinct
        CASE
            WHEN vp = 1 THEN 'VP'
            ELSE NULL
        END
    ) AS VP
FROM {self.table}
WHERE service_legal = 1 OR service_cm = 1 OR cm_juv_divert = 1
      OR cm_scan = 1 OR cm_scan_outreach = 1 OR hd = 1
      OR rjcc = 1 OR crws = 1 OR vp = 1
GROUP BY participant_id),

concat_services as (SELECT
    participant_id,
    CONCAT_WS(' + ',
        Legal,
        CM,
        YIP,
        SCaN,
        SCaN_Outreach,
        HD,
        RJCC,
        CRwS,
        VP
    ) AS concatenated_services
FROM (services_row))

select concatenated_services, count(concatenated_services), count(concatenated_services)/(SELECT COUNT(*) FROM concat_services) as percentage 
from concat_services
group by concatenated_services
order by count(concatenated_services) desc'''
        df = self.query_run(query)
        return df
    

    def programs_service_tally(self, service_column):
        '''
        Meant for use with a stints.classify_by_program like table. Counts the number of clients receiving a given service

        Parameters:
            Service column: the the column to tally ("service_legal", "service_cm", "cm_juv_divert", "cm_scan", "cm_scan_outreach", "hd", "hd_rct", "rjcc", "crws", "vp", "outreach")
        
        
        SQL Equivalent: 
            
            select COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS Count from stints.classify_by_program;
        '''
        if service_column.lower() == "outreach":
            query = f'''select count(distinct participant_id) from {self.table} where program_type like "%outreach%"'''
        else:
            query = f'''select COUNT(DISTINCT CASE WHEN {service_column} = 1 THEN participant_id END) AS Count from {self.table}'''
        
        df = self.query_run(query)
        return df
    
    def outreach_tally(self):
        '''
        counts clients receiving outreach
        '''
        query = f'''select count(distinct participant_id) from {self.table} where program_type like "%outreach%"'''
        df = self.query_run(query)
        return df
    
    def programs_sessions(self, timeframe = True, cm_outreach = "cm"):
        '''
        Counts clients who have at least one recorded session with a case manager or outreach worker
        
        Parameters:
            timeframe (Bool): whether to only count people who had at least one linkage between T1 and T2, defaults to True
            cm_outreach: which type of session to count. "cm" returns case management sessions, "outreach" returns outreach sessions. defaults to "cm"
        
        SQL Equivalent: 
        
            select count(distinct participant_id) from stints.classify_by_program 
            join civicore.case_sessions using(participant_id) 
            where session_casenote = "Casenote" and session_date BETWEEN '2023-08-01' and '2023-08-31';
        '''
        if cm_outreach.lower() == "cm":
            cm_outreach = f'"Casenote"'
        if cm_outreach.lower() == "outreach":
            cm_outreach = f'"Mentoring Session"'
        query = f'''select count(distinct participant_id) from {self.table} join civicore.case_sessions using(participant_id) where session_casenote = {cm_outreach}'''
        if timeframe is True:
            query = self.query_modify(str(query), f'''and session_date BETWEEN {self.q_t1} and {self.q_t2}''')
        df = self.query_run(query)
        return df
    
    def programs_session_tally(self, cm_outreach = "cm"):
        '''
        Counts successful and unsuccessful sessions among outreach or case managers

        Parameters:
            cm_outreach: which type of session to count. "cm" returns case management sessions, "outreach" returns outreach sessions. defaults to "cm"

        SQL Equivalent:

            select 
                COUNT(DISTINCT CASE WHEN (hours = 0 OR indirect_type = 'Unsuccessful Attempt') THEN case_session_id END) AS unsuccessful,
                COUNT(DISTINCT CASE WHEN hours > 0 AND (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') THEN case_session_id END) AS successful    
            from stints.classify_by_program
            join civicore.case_sessions using(participant_id)
            where (session_date BETWEEN '2023-08-01' and '2023-08-31') and session_casenote = "Casenote";
        '''
        if cm_outreach.lower() == "cm":
            cm_outreach = f'"Casenote"'
        if cm_outreach.lower() == "outreach":
            cm_outreach = f'"Mentoring Session"'
        query = f'''select 
	COUNT(DISTINCT CASE WHEN (hours = 0 OR indirect_type = 'Unsuccessful Attempt') THEN case_session_id END) AS unsuccessful,
    COUNT(DISTINCT CASE WHEN hours > 0 AND (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') THEN case_session_id END) AS successful
from {self.table}
join civicore.case_sessions using(participant_id)
where (session_date BETWEEN {self.q_t1} and {self.q_t2}) and session_casenote = {cm_outreach}'''
        df = self.query_run(query)
        return df
    
    def programs_by_demographic(self, demographic = None, percentages = False):
        '''
        Tallies program involvement by demographic feature("age", "race", "gender")

        Parameters:
            demographic: the demographic of choice ("age", "race", "gender")

        SQL Equivalent:

            select gender, 
                COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS HD,
                COUNT(DISTINCT CASE WHEN cm_juv_divert = 1 THEN participant_id END) AS juv_div,
                COUNT(DISTINCT CASE WHEN cm_scan = 1 THEN participant_id END) AS SCaN,
                COUNT(DISTINCT CASE WHEN rjcc = 1 THEN participant_id END) AS RJCC,
                COUNT(DISTINCT CASE WHEN grant_type regexp ".*JAC.*" THEN participant_id END) AS JAC,
                COUNT(DISTINCT CASE WHEN vp = 1 AND grant_type regexp ".*IDHS.*" THEN participant_id END) AS IDHS
                from stints.classify_by_program
                join civicore.participants using(participant_id) group by gender;
        '''
        if demographic == "age" and self.joined_participants == False:
            query = f'''select age, 
    COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS HD,
    COUNT(DISTINCT CASE WHEN cm_juv_divert = 1 THEN participant_id END) AS juv_div,
    COUNT(DISTINCT CASE WHEN cm_scan = 1 THEN participant_id END) AS SCaN,
    COUNT(DISTINCT CASE WHEN rjcc = 1 THEN participant_id END) AS RJCC,
    COUNT(DISTINCT CASE WHEN grant_type regexp ".*JAC.*" THEN participant_id END) AS JAC,
    COUNT(DISTINCT CASE WHEN vp = 1 AND grant_type regexp ".*IDHS.*" THEN participant_id END) AS IDHS
    from (select *, 
case when {self.q_t1} < DATE_ADD(birth_date, INTERVAL 18 YEAR) then "juvenile" else "emerging adult" end as "age"
from {self.table}
join (select participant_id, birth_date, race, gender from civicore.participants) x using(participant_id)) y
    group by age'''
        else:
            query = f'''select {demographic}, 
    COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS HD,
    COUNT(DISTINCT CASE WHEN cm_juv_divert = 1 THEN participant_id END) AS juv_div,
    COUNT(DISTINCT CASE WHEN cm_scan = 1 THEN participant_id END) AS SCaN,
    COUNT(DISTINCT CASE WHEN rjcc = 1 THEN participant_id END) AS RJCC,
    COUNT(DISTINCT CASE WHEN grant_type regexp ".*JAC.*" THEN participant_id END) AS JAC,
    COUNT(DISTINCT CASE WHEN vp = 1 AND grant_type regexp ".*IDHS.*" THEN participant_id END) AS IDHS
    from {self.table} 
    group by {demographic}'''
        if self.joined_participants == False and demographic != "age":
            query = self.query_modify(str(query), f'''join civicore.participants using(participant_id)''')
        df = self.query_run(query)
        if percentages == True:
            df = self.percentage_convert(df)
        return df
    
    def programs_session_length(self, cm_outreach = "cm", grouped = False, timeframe = True):
        '''
        Finds the length of case management or outreach sessions

        Parameters:
            cm_outreach: "cm" returns case management sessions, while "outreach" returns outreach sessions, defaults to "cm"
            grouped(Bool): True returns the number of sessions in assorted 15 minute intervals, while False finds the average length of sessions overall, and sessions excluding unsuccessful contact. Defaults to False
            timeframe (Bool): whether to only count people who had at least one linkage between T1 and T2, defaults to True
        
        SQL Equivalent:

            select 
                avg(hours) * 60 as overall,
                avg(case when hours > 0  and (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') then hours else null end) * 60 as successful    
            from stints.classify_by_program
            join civicore.case_sessions using(participant_id) where session_casenote = "Casenote" and session_date BETWEEN '2023-08-01' and '2023-08-31';
        '''
        if cm_outreach == "cm":
            cm_outreach = f'"Casenote"'
        if cm_outreach == "outreach":
            cm_outreach = f'"Mentoring Session"'
        if grouped is False:
            query = f'''select 
            avg(hours) * 60 as overall,
         avg(case when hours > 0  and (indirect_type IS NULL OR indirect_type NOT LIKE 'Unsuccessful Attempt') then hours else null end) * 60 as successful
    from {self.table}
    join civicore.case_sessions using(participant_id) where session_casenote = {cm_outreach}'''
            if timeframe is True:
                query = self.query_modify(str(query), f'''and session_date BETWEEN {self.q_t1} and {self.q_t2}''')
        if grouped is True:
            if timeframe is True:  
                query = f'''with hr_avg as (select 
        c.*,
        case 
        when hours = 0 or (contact_type = 'Unsuccessful Attempt') then "0"
        when hours > 0 and hours <= .25  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "0-.25"
        when hours >.25 and hours <= .5  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then ".26-.5"
        when hours > 0.5 and hours <= .75  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then ".51-.75"
        when hours > .75 and hours <= 1  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then ".76-1"
        when hours > 1 and hours <= 1.25  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1-1.25"
        when hours >1.25 and hours <= 1.5  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1.26-1.5"
        when hours > 1.5 and hours <= 1.75  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1.51-1.75"
        when hours > 1.75 and hours <= 2  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1.76-2"
        when hours > 2 then "2+"
        end as session_length
    from {self.table}
    join civicore.case_sessions c using(participant_id)
    where session_date BETWEEN {self.q_t1} and {self.q_t2} and session_casenote = {cm_outreach})

    select session_length, count(session_length)
    from hr_avg
    group by session_length
    order by case when session_length = '0' then 1
                when session_length = '0-.25' then 2
                when session_length = '.26-.5' then 3
                when session_length = '.51-.75' then 4
                when session_length = '.76-1' then 5
                when session_length = '1-1.25' then 6
                when session_length = '1.26-1.5' then 7
                when session_length = '1.51-1.75' then 8
                when session_length = '1.76-2' then 9
                when session_length = '2+' then 10
                else 0
                end asc'''
            else: 
                query = f'''with hr_avg as (select 
        c.*,
        case 
        when hours = 0 or (contact_type = 'Unsuccessful Attempt') then "0"
        when hours > 0 and hours <= .25  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "0-.25"
        when hours >.25 and hours <= .5  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then ".26-.5"
        when hours > 0.5 and hours <= .75  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then ".51-.75"
        when hours > .75 and hours <= 1  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then ".76-1"
        when hours > 1 and hours <= 1.25  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1-1.25"
        when hours >1.25 and hours <= 1.5  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1.26-1.5"
        when hours > 1.5 and hours <= 1.75  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1.51-1.75"
        when hours > 1.75 and hours <= 2  and (contact_type IS NULL OR contact_type NOT LIKE 'Unsuccessful Attempt') then "1.76-2"
        when hours > 2 then "2+"
        end as session_length
    from {self.table}
    join civicore.case_sessions c using(participant_id)
    where session_casenote = {cm_outreach})

    select session_length, count(session_length)
    from hr_avg
    group by session_length
    order by case when session_length = '0' then 1
                when session_length = '0-.25' then 2
                when session_length = '.26-.5' then 3
                when session_length = '.51-.75' then 4
                when session_length = '.76-1' then 5
                when session_length = '1-1.25' then 6
                when session_length = '1.26-1.5' then 7
                when session_length = '1.51-1.75' then 8
                when session_length = '1.76-2' then 9
                when session_length = '2+' then 10
                else 0
                end asc'''
        df = self.query_run(query)
        return df
    
    def programs_rct_neighborhoods(self):
        '''
        Returns a breakdown of RCT involvement by neighborhood

        SQL Equivalent:

            select
                count(distinct case when program_type regexp ".*RCT.*" then participant_id else null end) as Overall,
                count(distinct case when program_type regexp ".*RCT,.*" OR program_type = "RCT" OR program_type like "%, RCT" then participant_id else null end) as Lawndale,  
                count(distinct case when program_type regexp ".*RCT - BUILD.*" then participant_id else null end) as Austin,
                count(distinct case when program_type regexp ".*RCT - NLC.*"  then participant_id else null end) as Little_Village,
                count(distinct case when program_type regexp ".*RCT - Breakthrough.*" then participant_id else null end) as East_Garfield_Park,
                count(distinct case when program_type regexp ".*RCT - Target.*" then participant_id else null end) as South_Side
            from stints.classify_by_program;
        '''
        query = f'''select
count(distinct case when program_type regexp ".*RCT.*" then participant_id else null end) as Overall,
count(distinct case when program_type regexp ".*RCT,.*" OR program_type = "RCT" OR program_type like "%, RCT" then participant_id else null end) as Lawndale,
count(distinct case when program_type regexp ".*RCT - BUILD.*" then participant_id else null end) as Austin,
count(distinct case when program_type regexp ".*RCT - NLC.*"  then participant_id else null end) as Little_Village,
count(distinct case when program_type regexp ".*RCT - Breakthrough.*" then participant_id else null end) as East_Garfield_Park,
count(distinct case when program_type regexp ".*RCT - Target.*" then participant_id else null end) as South_Side
from {self.table}'''
        df = self.query_run(query)
        return df



class A2J(Queries):
    '''
    Sets up a table for running A2J reports

    Parameters:
        t1: start of the stint (formatted: "YYYY-MM-DD")
        t2: end of the stint (formatted: "YYYY-MM-DD")
    '''
    report_funcs = {
        "CASES_STARTED": ("cases", ("ended",)),
        "CASES_ENDED": ("cases", ("started",)),
        "LINKAGES": ("linkages", ())
    }

    def __init__(self, t1, t2, default_table="stints.classify_by_program"):
        super().__init__(t1, t2, default_table)
        self.table_update("a2j", update_default_table= True)
    
    def cases(self, started_ended = "started"):
        '''
        counts legal cases for A2J that either started or ended

        Parameters:
            started_ended: "started" counts cases started, "ended" is cases ended. Defaults to "started" 
        '''
        if started_ended.lower()== "started":
            started_ended = "arrest_date"
        else:
            started_ended = "case_outcome_date"
        query = f'''select {self.table}.*, o.offense_date, o.case_outcome_date, o.case_outcome, o.sentence
from participants.a2j
join civicore.legal o using(participant_id)
where {started_ended} between {self.q_t1} and {self.q_t2}'''
        df = self.query_run(query)

        return df
    
    def linkages(self):
        '''
        Counts linkages for A2J
        '''
        query = f'''select a.*, linkage_type, linkage_org, goal_area
from {self.table} a
join civicore.linkages l using(participant_id)
where linked_date between {self.q_t1} and {self.q_t2}'''
        df = self.query_run(query)
        return df
    


class Monthly_Report(Queries):
    program_grant_funcs = {
    "grant involvement": ("grant_tally", ()),
    "grants started": ("grant_tally", ("start_date",)),
    "grants ended": ("grant_tally", ("end_date",)),
    "Program Involvement by Days - All": ("grouped_by_time", ("program", False,True,)),
    "Program Involvement by Days - Closed": ("grouped_by_time", ("program", True, True,)),
    "PROGRAMS BY RACE": ("programs_by_demographic",("race", True,)),
    "PROGRAMS BY GENDER": ("programs_by_demographic",("gender",True, )),
    "PROGRAMS BY AGE": ("programs_by_demographic",("age",True,)),
    "ALL_DAYS_SERVED_BY_GRANT": ("grouped_by_time", ("grant", False,)),
    "CLOSED_DAYS_SERVED_BY_GRANT": ("grouped_by_time", ("grant", True,)),
    "PACKAGE OF SERVICES":("programs_packages",()),
    }
    legal_funcs = {
    "CiviCore.Legal Tally": ("legal_tally",()),
    "Case Pending Tally": ("legal_tally",(True,)),
    "Case Type - All Cases": ("legal_case_type", (False,False,"opened",True, "case")),
    "Case Type - Opened": ("legal_case_type", (False, True, "opened","case")),
    "Case Type - Closed": ("legal_case_type", (False, True, "closed","case")),
    "Rearrested": ("legal_rearrested",()),
    "Pending Cases": ("legal_pending_cases", ("t2",)),
    "Case Outcomes": ("legal_case_outcomes", ()),
    "Guilty Plea Outcomes": ("legal_plea_outcomes", (True,))
    }
    cm_outreach_funcs = {
    "receiving CM": ("programs_service_tally", ("service_cm",)),
    "Had CM Session in Month": ("programs_sessions", ()),
    "CM Session Count": ("programs_session_tally", ("cm",)),
    "CM Session Length": ("programs_session_length", ()),
    "CM Session Length (Grouped)": ("programs_session_length", ("cm", True, True)),
    "Had Outreach Session in Month": ("programs_sessions", ("outreach",)),
    "Outreach Session Count": ("programs_session_tally", ("outreach",)),
    "linkage tally": ("link_tally", (True, True, "linked_date",)),
    "services linked": ("link_goal_area", (True, True, "both",)) 
    }



    def __init__(self, t1, t2, default_table="stints.classify_by_program"):
        super().__init__(t1, t2, default_table)
        self.table_update("mreport", update_default_table= True)

    def grouped_by_status(self, program_grant = "grant"):
        '''
        Returns program or grant involvement by client status

        Parameters:
            program_grant: "program" tallies program involvement, "grant" tallies grant involvement. Defaults to "grant"
        '''
        if program_grant == "program":
            query = f'''select client_status, 
    COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS HD,
    COUNT(DISTINCT CASE WHEN cm_juv_divert = 1 THEN participant_id END) AS juv_div,
    COUNT(DISTINCT CASE WHEN rjcc = 1 THEN participant_id END) AS RJCC,
    COUNT(DISTINCT CASE WHEN cm_scan = 1 THEN participant_id END) AS SCaN,
    COUNT(DISTINCT CASE WHEN vp = 1 THEN participant_id END) AS vp,
    COUNT(DISTINCT CASE WHEN grant_type LIKE "%JAC%" THEN participant_id END) AS JAC,
    COUNT(DISTINCT CASE WHEN vp = 1 AND grant_type LIKE "%IDHS%" THEN participant_id END) AS IDHS
    from {self.table}
    group by client_status
    UNION ALL
    SELECT
    "new client" as client_status,
    COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS HD,
    COUNT(DISTINCT CASE WHEN cm_juv_divert = 1 THEN participant_id END) AS juv_div,
    COUNT(DISTINCT CASE WHEN rjcc = 1 THEN participant_id END) AS RJCC,
    COUNT(DISTINCT CASE WHEN cm_scan = 1 THEN participant_id END) AS SCaN,
    COUNT(DISTINCT CASE WHEN vp = 1 THEN participant_id END) AS vp,
    COUNT(DISTINCT CASE WHEN grant_type LIKE "%JAC%" THEN participant_id END) AS JAC,
    COUNT(DISTINCT CASE WHEN vp = 1 AND grant_type LIKE "%IDHS%" THEN participant_id END) AS IDHS
    from {self.table}
    where new_client = "new client"
    ORDER BY case when client_status = 'active client' then 1
    when client_status = 'new client' then 2
    when client_status = 'closed out' then 3
    end asc'''
        if program_grant == "grant":
            query = f'''select client_status, 
COUNT(DISTINCT CASE WHEN grant_type LIKE "%A2J%" THEN participant_id END) AS A2J,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%IDHS%" THEN participant_id END) AS IDHS,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%JAC%" THEN participant_id END) AS JAC,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%SCAN%" THEN participant_id END) AS SCAN,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%CRwS%" THEN participant_id END) AS CRwS,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%YIP%" THEN participant_id END) AS YIP
from {self.table}
group by client_status

UNION ALL
SELECT
	"new client" as client_status,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%A2J%" THEN participant_id END) AS A2J,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%IDHS%" THEN participant_id END) AS IDHS,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%JAC%" THEN participant_id END) AS JAC,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%SCAN%" THEN participant_id END) AS SCAN,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%CRwS%" THEN participant_id END) AS CRwS,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%YIP%" THEN participant_id END) AS YIPS
from {self.table}
where new_client = "new client"
ORDER BY case when client_status = 'active client' then 1
              when client_status = 'new client' then 2
              when client_status = 'closed out' then 3
              end asc'''
        df = self.query_run(query)
        return df
        
    def grouped_by_time(self, program_grant = "grant",closed_only = False, percentages = True):
        '''
        Returns program or grant involvement by client status

        Parameters:
            program_grant: "program" tallies program involvement, "grant" tallies grant involvement. Defaults to "grant"
            closed_only(Bool): True only tallies closed clients
            percentages(Bool): True auto-converts to percentages. Defaults to True 
        '''
        if program_grant == "program":
            query = f'''select days_served, 
    COUNT(DISTINCT CASE WHEN hd = 1 THEN participant_id END) AS HD,
    COUNT(DISTINCT CASE WHEN cm_juv_divert = 1 THEN participant_id END) AS juv_div,
    COUNT(DISTINCT CASE WHEN rjcc = 1 THEN participant_id END) AS RJCC,
    COUNT(DISTINCT CASE WHEN cm_scan = 1 THEN participant_id END) AS SCaN,
    COUNT(DISTINCT CASE WHEN grant_type LIKE "%JAC%" THEN participant_id END) AS JAC,
    COUNT(DISTINCT CASE WHEN vp = 1 AND grant_type LIKE "%IDHS%" THEN participant_id END) AS IDHS
    from {self.table}
    group by days_served
    ORDER BY case when days_served = '0-45' then 1
    when days_served = '46-90' then 2
    when days_served = '91-180' then 3
    when days_served = '181-270' then 4
    else 5
    end asc'''
        if program_grant == "grant":
            query = f'''select days_served, 
COUNT(DISTINCT CASE WHEN grant_type LIKE "%A2J%" THEN participant_id END) AS A2J,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%IDHS%" THEN participant_id END) AS IDHS,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%JAC%" THEN participant_id END) AS JAC,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%SCAN%" THEN participant_id END) AS SCAN,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%CRwS%" THEN participant_id END) AS CRwS,
COUNT(DISTINCT CASE WHEN grant_type LIKE "%YIP%" THEN participant_id END) AS YIP
from {self.table}
group by days_served
ORDER BY case when days_served = '0-45' then 1
when days_served = '46-90' then 2
when days_served = '91-180' then 3
when days_served = '181-270' then 4
else 5
end asc'''
        if closed_only == True:
            query = self.query_modify(str(query), f'''where client_status = "closed out"''')
        df = self.query_run(query)
        if percentages == True:
            df = self.percentage_convert(df)
        return df
    
class HD(Queries):
    def __init__(self, t1, t2, print_SQL = True, clipboard = False, default_table="stints.classify_by_program"):
        super().__init__(t1, t2, print_SQL, clipboard, default_table)
        self.table_update("hd", update_default_table= True)

class JAC_IDHS(Queries):
    """   
    Sets up a table for running JAC/IDHS reports

    
    Parameters:
        t1: start of the stint (formatted: "YYYY-MM-DD")
        t2: end of the stint (formatted: "YYYY-MM-DD")
        print_sql: whether to print the SQL statements when run, defaults to True
        clipboard: whether to copy the output table to your clipboard, defaults to False
        grant: which grant to include, either "jac" or "idhs"
        """
    JAC_Smartsheet = {
    'participant count': ("idhs_tally",()),
    'genders': ('dem_race_gender',(False,'gender',)),
    'races': ('dem_race_gender',(False, 'race',)),
    'ages': ('dem_age',()),
    'services': ('idhs_service_tally',()),
    'avg hours spent on CM': ('jac_cm_hours',()),
    'avg number of CM sessions': ('jac_cm_sessions',()),
    'transportation assistance': ('jac_transpo_assist',()),
    'connected to other providers': ('jac_linked_participant_tally', ()),
    'referrals': ('idhs_linkages',())
}
    CVI = {
        'total participants': ('idhs_tally',()),
        'ages': ('idhs_ages',()),
        'races': ('idhs_race_gender', ('race',)),
        'genders': ('idhs_race_gender', ('gender',)),
        'languages': ('idhs_language',()),
        'services':('idhs_service_tally',()),
        'referrals':('idhs_linkages',()),
        'number of mediations': ('mediation_tally',()),
        'CPIC notifications': ('idhs_incidents_detailed', (True,)),
        'non-CPIC notifications': ('idhs_incidents_detailed', (False,)),
        'time spent on mediations': ('mediation_time_spent',())
    }

    PPR = {'new and continuing clients': ('idhs_tally', (False,)),
        'closed clients': ('idhs_tally', (True,)),
        'outreach and legal': ('idhs_service_tally',()),
        'race': ('dem_race_gender', (False, 'race',)),
        'gender': ('dem_race_gender', (False, 'gender',)),
        'ages': ('idhs_ages', (True,)),
        'eligibility': ('idhs_class_notes',(True,)),
        'cm linkages': ('idhs_linkages',()),
        'initial mediations': ('mediation_tally', (False,)),
        'followup mediations': ('mediation_tally', (True,)),
        'incidents': ('incident_tally',())
        }

    def __init__(self, t1, t2, print_SQL = True, clipboard = False, default_table="stints.classify_by_program", grant = 'idhs'):
        super().__init__(t1, t2, print_SQL, clipboard, default_table)
        if grant.lower() == "idhs":
            self.program = 'idhs'
            self.table_update("idhs", update_default_table= True)
        if grant.lower() == "jac":
            self.program = 'jac'
            self.table_update("jac", update_default_table= True)

    def idhs_ages(self, PPR = False):
        '''
        Returns ages of IDHS clients in the desired grouping

        Parameters:
            PPR: if True, groups according to PPR age breakdown. Defaults to False
        '''
        if PPR == False:
            query = f'''select 
        new_client,
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 0 AND 17 then participant_id else null end) as "Under 18",
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 18 AND 25 then participant_id else null end) as "18 to 24"
    from {self.table}
    join civicore.participants using(participant_id)
    group by new_client'''
        else:
            query = f'''select 
        new_client,
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 0 AND 5 then participant_id else null end) as "0 to 5",
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 6 AND 10 then participant_id else null end) as "6 to 10",
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 11 AND 13 then participant_id else null end) as "11 to 13",
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 14 AND 17 then participant_id else null end) as "14 to 17",
        count(distinct case when TIMESTAMPDIFF(YEAR, birth_date, active_date) BETWEEN 18 AND 25 then participant_id else null end) as "18 to 24"
    from {self.table}
    join civicore.participants using(participant_id)
    group by new_client
'''
        df = self.query_run(query)
        return df
    
    def idhs_tally(self, closed = False):
        '''
        counts number of people on grant

        Parameters:
            closed(Bool): if True, only returns closed clients. Defaults to False
        '''
        if closed is False: 
            query = f'''select new_client, count(distinct participant_id)
    from {self.table}
    group by new_client'''
        else:
            query = f'''select count(distinct participant_id) from {self.table}
where end_date between {self.q_t1} and {self.q_t2} or (closed_date between {self.q_t1} and {self.q_t2})'''
        df = self.query_run(query)
        return df
    
    def idhs_class_notes(self, new = False):
        '''
        returns those eligibility acronyms

        Parameters:
            new(Bool): if True, only returns new clients. Defaults to False
        '''
        if new == False:
            query = f'''with class as (select * from (select distinct participant_id
    from {self.table}) j
    join civicore.classes using(participant_id)
    where class regexp '2023 IDHS VP.*' and (unenroll_date is null or unenroll_date between {self.q_t1} and {self.q_t2}))

    SELECT acronym, COUNT(*) AS count
    FROM (
        SELECT TRIM(REPLACE(REGEXP_SUBSTR(CAST(summary_note AS CHAR), '[[:alnum:]]+', 1, n.n), ' ', '')) AS acronym
        FROM class
        JOIN (
            SELECT a.N + b.N * 10 + 1 AS n
            FROM
                (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) a,
                (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) b
            ORDER BY n
        ) n ON CHAR_LENGTH(CAST(summary_note AS CHAR)) - CHAR_LENGTH(REGEXP_REPLACE(CAST(summary_note AS CHAR), '[[:alnum:]]+', '')) >= n.n
    ) AS Acronyms
    WHERE acronym != ''
    GROUP BY acronym'''
        if new == True:
            query = f'''with class as (select * from (select distinct participant_id
    from {self.table} where start_date > {self.q_t1}) j
    join civicore.classes using(participant_id)
    where class regexp '2023 IDHS VP.*' and (unenroll_date is null or unenroll_date between {self.q_t1} and {self.q_t2}))

    SELECT acronym, COUNT(*) AS count
    FROM (
        SELECT TRIM(REPLACE(REGEXP_SUBSTR(CAST(summary_note AS CHAR), '[[:alnum:]]+', 1, n.n), ' ', '')) AS acronym
        FROM class
        JOIN (
            SELECT a.N + b.N * 10 + 1 AS n
            FROM
                (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) a,
                (SELECT 0 AS N UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) b
            ORDER BY n
        ) n ON CHAR_LENGTH(CAST(summary_note AS CHAR)) - CHAR_LENGTH(REGEXP_REPLACE(CAST(summary_note AS CHAR), '[[:alnum:]]+', '')) >= n.n
    ) AS Acronyms
    WHERE acronym != ''
    GROUP BY acronym'''
        df = self.query_run(query)
        return df
    
    def idhs_race_gender(self, race_gender = 'race'):
        '''
        Returns count of race/gender, distinguishing between new/continuing clients

        Parameters:
            race_gender: 'race' returns client races, 'gender' returns client genders. Defaults to 'race'
        '''
        query = f'''select new_client, {race_gender}, count({race_gender})
from {self.table}
join civicore.participants using(participant_id)
group by new_client, {race_gender}'''
        df = self.query_run(query)
        return df
        
    def idhs_language(self):
        '''
        counts participant primary languages
        '''
        query = f'''select new_client, language_primary, count(distinct participant_id)
from {self.table}
join civicore.participants using(participant_id)
group by new_client, language_primary'''
        df = self.query_run(query)
        return df
    
    def idhs_service_tally(self):
        '''
        counts clients receiving services from the chosen grant
        '''
        query = f'''select new_client, count(distinct case when program_type regexp ".*{self.program} - Outreach.*" then participant_id else null end) as outreach, 
count(distinct case when program_type regexp ".*{self.program} - Case.*" then participant_id else null end) as CM,
count(distinct case when program_type regexp ".*{self.program} - Legal.*" then participant_id else null end) as legal
from {self.table}
group by new_client'''
        df = self.query_run(query)
        return df

    
    def idhs_linkages (self):
        '''
        returns goal areas for clients receiving a grant's case management
        '''
        query = f'''with separated_goals as (SELECT participant_id, first_name, last_name, linkage_id, linkage_type, linkage_org, linked_date, hub_initiated, new_client, SUBSTRING_INDEX(SUBSTRING_INDEX(goal_area, ', ', n), ', ', -1) AS separated_area
FROM (select participant_id, first_name, last_name, linkage_type, linkage_org, linkage_id, goal_area, linked_date, hub_initiated, new_client from (select * from 
{self.table}
where program_type regexp '.*{self.program} - case.*') p
join civicore.linkages l using(participant_id) where hub_initiated = "Yes") s
JOIN (
    SELECT 1 AS n UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5 UNION ALL
    SELECT 6 UNION ALL
    SELECT 7 UNION ALL
    SELECT 8 UNION ALL
    SELECT 9
) AS numbers
ON CHAR_LENGTH(goal_area) - CHAR_LENGTH(REPLACE(goal_area, ',', '')) >= n - 1)

select separated_area, count(distinct case when new_client = "new" then linkage_id end) as new_links, count(distinct case when new_client = "continuing" then linkage_id end) as cont_links
from separated_goals
where linked_date between {self.q_t1} and {self.q_t2}
group by separated_area'''
        df = self.query_run(query)
        return df
    
    def idhs_incidents_detailed(self, CPIC = True):
        '''
        returns incident analysis. need to update code to specify neighborhoods.


        '''
        if CPIC == True:
            query = f'''SELECT type_incident,
count(case when num_deceased > 0 then incident_id else null end) as fatal,
count(case when num_deceased = 0 then incident_id else null end) as non_fatal
FROM civicore.critical_incidents
where how_hear regexp '.*cpic.*' and date_incident between {self.q_t1} and {self.q_t2}
group by type_incident'''
        else:
            query = f'''select how_hear, count(incident_id) from civicore.critical_incidents
where date_incident between {self.q_t1} and {self.q_t2} and how_hear not regexp '.*cpic.*'
group by how_hear'''
        df = self.query_run(query)
        return df
    
    def jac_cm_hours(self):
        '''
        gets average case management hours for JAC clients
        '''

        query = f'''select avg(hours) as overall_avg from
(select participant_id, sum(hours) as hours from 
(select distinct participant_id 
from {self.table}
where program_type REGEXP '.*{self.program} - Case Management.*') l
join civicore.case_sessions using(participant_id)
where session_date BETWEEN {self.q_t1} and {self.q_t2} AND session_casenote = "casenote"
group by participant_id) ll'''
        df = self.query_run(query)
        return df
    
    def jac_cm_sessions(self):
        '''
        gets average case management sessions for JAC clients
        '''
        query = f'''select count(distinct case_session_id)/count(distinct participant_id) from (select * from (select distinct participant_id 
from {self.table}
where program_type REGEXP '.*{self.program} - Case Management.*') l
join civicore.case_sessions using(participant_id)
where session_date BETWEEN {self.q_t1} and {self.q_t2} AND session_casenote = "casenote" and hours > 0 and (indirect_type != 'Unsuccessful Attempt' or indirect_type IS NULL)) ll;'''
        df = self.query_run(query)
        return df
    
    def jac_transpo_assist(self):
        '''
        counts clients marked as both jac and CRwS
        '''
        query = f'''select count(distinct participant_id)
from participants.jac
where program_type like "%JAC%" and crws = 1'''
        df = self.query_run(query)
        return df
    
    def jac_linked_participant_tally(self):
        '''
        counts jac participants with a started linkage
        '''
        query = f'''select count(distinct participant_id) from participants.jac
join civicore.linkages l using (participant_id)
where l.start_date IS NOT NULL'''
        df = self.query_run(query)
        return df
    