from eralchemy import render_er

# Definindo o modelo
er_model = '''
[d_date_dim]
date_id* (PK)
date
year
quarter
month
day
week

[d_states_dim]
state_id* (PK)
state_name
state_code

[d_counties_dim]
county_id* (PK)
county_name
state_id (FK - d_states_dim.state_id)

[d_cities_dim]
city_id* (PK)
city_name
county_id (FK - d_counties_dim.county_id)

[d_colleges_dim]
college_id* (PK)
college_name
city_id (FK - d_cities_dim.city_id)

[d_prisons_dim]
prison_id* (PK)
prison_name
county_id (FK - d_counties_dim.county_id)

[f_us_states]
date_id (FK - d_date_dim.date_id)
state_id (FK - d_states_dim.state_id)
cases
deaths

[f_us_counties]
date_id (FK - d_date_dim.date_id)
county_id (FK - d_counties_dim.county_id)
cases
deaths

[f_colleges]
date_id (FK - d_date_dim.date_id)
college_id (FK - d_colleges_dim.college_id)
cases
cases_2021

[f_prisons]
date_id (FK - d_date_dim.date_id)
prison_id (FK - d_prisons_dim.prison_id)
cases
cases_staff
'''

# Criando o diagrama
render_er(er_model, 'er_diagram.png')
