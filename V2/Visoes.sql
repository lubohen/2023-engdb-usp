-- RETIRA VALORES ACUMULADOS

WITH UNT as (SELECT
  date,
  state,
  fips,
  cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) AS new_cases,
  deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) AS new_deaths
FROM
  "nytimes-raw-us_states"
ORDER BY
  state, date)
SELECT state, fips, sum(new_cases) as "sumofcases", sum(new_deaths) as "sumofdeaths" from UNT
--where "unt"."fips" = 53
GROUP BY state,fips
ORDER BY sumofcases;

--
-- CRIAÇÃO DAS TABELAS SEM VALORES ACUMULADOS

CREATE TABLE IF NOT EXISTS "covid-refined"."nytimes-refined-us_states_unt" WITH (
	format = 'ORC',
	external_location = 's3://usp-prjint-covid/specialized/us_states_unt/',
	partitioned_by = ARRAY [ 'state' ]
) AS
SELECT
  date,
  fips,
  cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) AS new_cases,
  deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) AS new_deaths,
  state
FROM
  "nytimes-trusted-us_states"
ORDER BY
  date;

-- Sem valores negativos:

SELECT
  date,
  fips,
  CASE WHEN cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_cases,
  CASE WHEN deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_deaths,
  state
FROM
  "covid-trusted"."nytimes-trusted-us_states"
ORDER BY
  date;

----

CREATE TABLE IF NOT EXISTS "covid-refined"."nytimes-refined-us_counties_unt" WITH (
	format = 'ORC',
	external_location = 's3://usp-prjint-covid/specialized/us_counties_unt/',
	partitioned_by = ARRAY [ 'year','month' ]
) AS
SELECT
  county,
  state,
  fips,
  CASE WHEN cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_cases,
  CASE WHEN deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_deaths,
  date,
  YEAR(date) AS year,
  MONTH(date) AS month
FROM
  "nytimes-trusted-counties"
ORDER BY
  date;

  --- 

  CREATE TABLE IF NOT EXISTS "covid-refined"."nytimes-refined-us_counties_unt" WITH (
	format = 'ORC',
	external_location = 's3://usp-prjint-covid/specialized/us_counties_unt/',
	partitioned_by = ARRAY [ 'year','month' ]
) AS
SELECT
  county,
  state,
  fips,
  CASE WHEN cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_cases,
  CASE WHEN deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_deaths,
  date,
  YEAR(date) AS year,
  MONTH(date) AS month
FROM
  "nytimes-trusted-counties"
ORDER BY
  date;