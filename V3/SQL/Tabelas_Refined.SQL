-- CRIAÇÃO DAS TABELAS SEM VALORES ACUMULADOS

CREATE TABLE IF NOT EXISTS "covid-refined"."nytimes-refined-us_states_unt" WITH (
	format = 'ORC',
	external_location = 's3://usp-prjint-covid/specialized/us_states_unt/',
	partitioned_by = ARRAY [ 'state' ]
) AS
SELECT
  date,
  fips,
  CASE WHEN cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE cases - LAG(cases, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_cases,
  CASE WHEN deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) < 0 THEN 0 ELSE deaths - LAG(deaths, 1, 0) OVER (PARTITION BY state ORDER BY date) END AS new_deaths,
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

  --- condados_unificados

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
  

-- Spec County from Mask use

  CREATE TABLE IF NOT EXISTS "covid-refined"."nytimes-refined-us_mask_county" WITH (
	format = 'ORC',
	external_location = 's3://usp-prjint-covid/specialized/us_mask_county/',
) AS
SELECT DISTINCT (T1.county), T1.fips, T1.state, T2.never, T2.rarely, T2.sometimes, T2.frequently, T2.always FROM "covid-trusted"."nytimes-trusted-counties" T1
LEFT JOIN "covid-trusted"."nytimes-trusted-mask" T2
ON T1.fips = T2.countyfp;