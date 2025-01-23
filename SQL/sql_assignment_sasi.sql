use family;
create table table1 (name varchar(3),
                            age int, 
                            city varchar(5), 
                            zipcode varchar(8),
                            phone int,
                            state_code varchar(2),
                            order_id varchar(36)
                            );
create table table2 ( state_code int,
                           County varchar(6),
                            order_date datetime,
                            phone int,
                            order_id varchar(36),
                            name varchar(3)
                            );
                            
                            
                            
                            
select * from table1;
select* from table2;
-- join the two tables on State_code and name (3) and as table3 --   single line comments -- text -- multi line comments /* text */

select count(*) from table1;

drop table table1;
drop table table2;
drop table table3;
select count(*) from table1;
select count(*) from table2;
CREATE TABLE table3 AS
SELECT 
    t1.state_code AS state_code_t1, 
    t1.name AS name_t1, 
    t1.age, 
    t1.city, 
    t1.zipcode, 
    t1.phone AS phone_t1, 
    t1.order_id AS order_id_t1, 
    t2.state_code AS state_code_t2, 
    t2.name AS name_t2, 
    t2.county, 
    t2.order_date, 
    t2.phone AS phone_t2, 
    t2.order_id AS order_id_t2
FROM 
    table1 t1
LEFT JOIN 
    table2 t2
ON 
    t1.state_code = t2.state_code 
    AND t1.name = t2.name

UNION 

SELECT 
    t1.state_code AS state_code_t1, 
    t1.name AS name_t1, 
    t1.age, 
    t1.city, 
    t1.zipcode, 
    t1.phone AS phone_t1, 
    t1.order_id AS order_id_t1, 
    t2.state_code AS state_code_t2, 
    t2.name AS name_t2, 
    t2.county, 
    t2.order_date, 
    t2.phone AS phone_t2, 
    t2.order_id AS order_id_t2
FROM 
    table1 t1
RIGHT JOIN 
    table2 t2
ON 
    t1.state_code = t2.state_code 
    AND t1.name = t2.name;
    select count(*) from table3;
    select * from table3;
    
    --  Create a table table5 with records from states TX, CA, AZ, NY, FL--

create table table5 as select * from table3 where state_code_t1 in ( 'TX', 'CA', 'AZ', 'NY', 'FL' ) OR 
STATE_CODE_T2 IN ('TX', 'CA', 'AZ', 'NY', 'FL' );

select * from table5;

-- b) To find the State with most orders using order_id--

SELECT * FROM TABLE3;
SELECT STATE_CODE_T2, COUNT(ORDER_ID_T1), COUNT(ORDER_ID_T2) FROM TABLE3 
GROUP BY STATE_CODE_T1, STATE_CODE_T2;

SELECT state_code, SUM(order_count) AS total_orders
FROM (
    SELECT STATE_CODE_T1 AS state_code, COUNT(ORDER_ID_T1) AS order_count
    FROM TABLE3
    GROUP BY STATE_CODE_T1
    UNION ALL
    SELECT STATE_CODE_T2 AS state_code, COUNT(ORDER_ID_T2) AS order_count
    FROM TABLE3
    GROUP BY STATE_CODE_T2
) AS combined_orders
GROUP BY state_code
ORDER BY total_orders DESC
LIMIT 1;

-- To find the State with most orders by Year using order_id --

SELECT * FROM TABLE3;
WITH yearly_orders AS (
    SELECT 
        state_code, 
        YEAR(order_date) AS order_year, 
        COUNT(order_id) AS total_orders
    FROM (
        SELECT 
            state_code_t1 AS state_code, 
            order_date, 
            order_id_t1 AS order_id
        FROM TABLE3
        WHERE order_id_t1 IS NOT NULL -- Ensure valid orders
        UNION ALL
        SELECT 
            state_code_t2 AS state_code, 
            order_date, 
            order_id_t2 AS order_id
        FROM TABLE3
        WHERE order_id_t2 IS NOT NULL -- Ensure valid orders
    ) AS combined_orders
    GROUP BY state_code, order_year
),
ranked_states AS (
    SELECT 
        state_code,
        order_year,
        total_orders,
        ROW_NUMBER() OVER (PARTITION BY order_year ORDER BY total_orders DESC) AS rank_num
    FROM yearly_orders
)
SELECT 
    state_code, 
    order_year, 
    total_orders
FROM ranked_states
WHERE rank_num = 1
ORDER BY order_year DESC;

-- To find the State with most orders by Year and also City using order_id--

SELECT * FROM TABLE3;
WITH yearly_orders AS (
    SELECT 
        state_code, 
        YEAR(order_date) AS order_year, 
        COUNT(order_id) AS total_orders
    FROM (
        SELECT 
            state_code_t1 AS state_code, 
            order_date, 
            order_id_t1 AS order_id
        FROM TABLE3
        WHERE order_id_t1 IS NOT NULL -- Ensure valid orders
        UNION ALL
        SELECT 
            state_code_t2 AS state_code, 
            order_date, 
            order_id_t2 AS order_id
        FROM TABLE3
        WHERE order_id_t2 IS NOT NULL -- Ensure valid orders
    ) AS combined_orders
    GROUP BY state_code, order_year
),
ranked_states AS (
    SELECT 
        state_code,
        order_year,
        total_orders,
        ROW_NUMBER() OVER (PARTITION BY order_year ORDER BY total_orders DESC) AS rank_num
    FROM yearly_orders
)
SELECT 
    state_code, 
    order_year, 
    total_orders
FROM ranked_states
WHERE rank_num = 1
ORDER BY order_year DESC;
------------------------------------------
 -- d) To find the State with most orders by Year and also City using order_id --


 WITH OrdersPerState AS (
    SELECT
        YEAR(order_date) AS order_year,
        state_code_t1 AS state,
        COUNT(order_id_t1) AS total_orders
    FROM
        table3
    GROUP BY
        YEAR(order_date), state_code_t1
),
StateWithMostOrders AS (
    SELECT
        order_year,
        state,
        total_orders,
        RANK() OVER (PARTITION BY order_year ORDER BY total_orders DESC) AS rank_state
    FROM
        OrdersPerState
),
OrdersPerCity AS (
    SELECT
        YEAR(order_date) AS order_year,
        state_code_t1 AS state,
        city,
        COUNT(order_id_t1) AS total_orders
    FROM
        table3
    GROUP BY
        YEAR(order_date), state_code_t1, city
),
CityWithMostOrdersInTopState AS (
    SELECT
        c.order_year,
        c.state,
        c.city,
        c.total_orders,
        RANK() OVER (PARTITION BY c.order_year, c.state ORDER BY c.total_orders DESC) AS rank_city
    FROM
        OrdersPerCity c
    JOIN
        StateWithMostOrders s
    ON
        c.order_year = s.order_year AND c.state = s.state
    WHERE
        s.rank_state = 1
)
SELECT
    s.order_year,
    s.state AS state_with_most_orders,
    c.city AS city_with_most_orders
FROM
    StateWithMostOrders s
JOIN
    CityWithMostOrdersInTopState c
ON
    s.order_year = c.order_year AND s.state = c.state
WHERE
    s.rank_state = 1 AND c.rank_city = 1;

-- Create a query to find the length of the order_id from table 3 and also split it into parts and display the length of each part--

SELECT
    order_id_t1,
    LENGTH(order_id_t1) AS total_length,
    LENGTH(SUBSTRING_INDEX(order_id_t1, '-', 1)) AS part1_length, -- First part
    LENGTH(SUBSTRING_INDEX(SUBSTRING_INDEX(order_id_t1, '-', 2), '-', -1)) AS part2_length, -- Second part
    LENGTH(SUBSTRING_INDEX(SUBSTRING_INDEX(order_id_t1, '-', 3), '-', -1)) AS part3_length, -- Third part
    LENGTH(SUBSTRING_INDEX(order_id_t1, '-', -1)) AS part4_length,-- Fourth part
    order_id_t2,
    LENGTH(order_id_t2) AS total_length,
    LENGTH(SUBSTRING_INDEX(order_id_t2, '-', 1)) AS part1_length, -- First part
    LENGTH(SUBSTRING_INDEX(SUBSTRING_INDEX(order_id_t2, '-', 2), '-', -1)) AS part2_length, -- Second part
    LENGTH(SUBSTRING_INDEX(SUBSTRING_INDEX(order_id_t2, '-', 3), '-', -1)) AS part3_length, -- Third part
    LENGTH(SUBSTRING_INDEX(order_id_t2, '-', -1)) AS part4_length -- Fourth part
    
FROM
    table3;
--------------------------------------------
select  order_id_t1,
    LENGTH(order_id_t1) AS total_length,
    (SUBSTRING_INDEX(order_id_t1, '-', -1))
    from table3;
