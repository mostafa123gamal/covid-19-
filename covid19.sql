create table CovidVaccinations(
	iso_code varchar (15),
	continent varchar (15),
	location varchar (50),
	date date ,
	new_tests int ,
	total_tests int ,
	total_tests_per_thousand decimal ,
	new_tests_per_thousand decimal,
	new_tests_smoothed int ,
	new_tests_smoothed_per_thousand decimal,
	positive_rate decimal,
	tests_per_case decimal,
	tests_units varchar (50),
	total_vaccinations int,
	people_vaccinated int,
	people_fully_vaccinated int,
	new_vaccinations int	,
	new_vaccinations_smoothed int,
	total_vaccinations_per_hundred decimal ,
	people_vaccinated_per_hundred decimal,
	people_fully_vaccinated_per_hundred decimal,
	new_vaccinations_smoothed_per_million int,
	stringency_index decimal,
	population_density decimal,
	median_age decimal,
	aged_65_older decimal,
	aged_70_older decimal,
	gdp_per_capita decimal,
	extreme_poverty decimal,
	cardiovasc_death_rate decimal,
	diabetes_prevalence decimal,
	female_smokers decimal,
	male_smokers  decimal,
	handwashing_facilities decimal ,
	hospital_beds_per_thousand decimal,
	life_expectancy decimal,
	human_development_index decimal

)

copy CovidVaccinations from 'F:\data science\sql\projects\covid 1\CovidVaccinations.csv'DELIMITER ',' CSV HEADER;


--/////////////////////////
CREATE TABLE CovidDIE(
    iso_code varchar (15),
	continent varchar (15),
	location varchar (50),
	date DATE ,
	total_cases DECIMAL ,
	new_cases INT ,
	new_cases_smoothed DECIMAL  ,
	total_deaths DECIMAL 	,
	new_deaths INT ,
	new_deaths_smoothed DECIMAL ,	
	total_cases_per_million DECIMAL	,
	new_cases_per_million DECIMAL,
	new_cases_smoothed_per_million DECIMAL,
	total_deaths_per_million DECIMAL,
	new_deaths_per_million DECIMAL	,
	new_deaths_smoothed_per_million DECIMAL,
	reproduction_rate DECIMAL,
	icu_patients INT ,
	icu_patients_per_million DECIMAL	,
	hosp_patients	INT ,
	hosp_patients_per_million DECIMAL	,
	weekly_icu_admissions DECIMAL,
	weekly_icu_admissions_per_million DECIMAL,
	weekly_hosp_admissions DECIMAL,
	weekly_hosp_admissions_per_million DECIMAL	,
	new_tests INT ,
	total_tests INT  ,
	total_tests_per_thousand DECIMAL,
	new_tests_per_thousand DECIMAL,
	new_tests_smoothed INT ,
	new_tests_smoothed_per_thousand DECIMAL,
	positive_rate DECIMAL,
	tests_per_case  DECIMAL,
	tests_units VARCHAR(50),
	total_vaccinations INT ,
	people_vaccinated INT 	,
	people_fully_vaccinated	INT,
	new_vaccinations INT ,
	new_vaccinations_smoothed INT ,
	total_vaccinations_per_hundred DECIMAL,
	people_vaccinated_per_hundred DECIMAL,
	people_fully_vaccinated_per_hundred DECIMAL	,
	new_vaccinations_smoothed_per_million INT  ,
	stringency_index DECIMAL,
	population DECIMAL    ,
	population_density DECIMAL	,
	median_age	DECIMAL,
	aged_65_older	DECIMAL,
	aged_70_older DECIMAL,
	gdp_per_capita DECIMAL,
	extreme_poverty	DECIMAL,
	cardiovasc_death_rate DECIMAL,
	diabetes_prevalence DECIMAL,
	female_smokers	DECIMAL,
	male_smokers DECIMAL,
	handwashing_facilities DECIMAL 	,
	hospital_beds_per_thousand DECIMAL	,
	life_expectancy	DECIMAL,
	human_development_index DECIMAL
)


COPY CovidDIE FROM 'F:\data science\sql\projects\covid 1\CovidDeaths.csv' DELIMITER ',' CSV HEADER ;

SELECT *
FROM CovidDIE;

--- select data that we are going to be using 
SELECT location , date , total_cases, new_cases,total_deaths, population
FROM CovidDIE
order by 1,2;


--- looking at total cases vs total deaths

SELECT 
    location,
    date,
    total_cases,
    total_deaths,
    (total_deaths / total_cases) * 100 AS deathpercentage
FROM 
    CovidDIE
WHERE 
    location LIKE '%Afghanistan'
ORDER BY 
    1, 2;

--- looking at total cases vs population
-- show what percentage of population
SELECT 
    location,
    date,
    total_cases,
    population,
    (total_cases / population) * 100 AS populationpercentage
FROM 
    CovidDIE
WHERE  location LIKE '%America'
ORDER BY 
    1, 2;

--- looking at countries with highest infection rate compares to  population

SELECT 
   location,
   population,
   max(total_cases)as highestinfect,
   max ((total_cases / population)) * 100 AS populationpercentagepopulationinfected
FROM 
    CovidDIE
--WHERE location LIKE '%America'
group by location ,population

ORDER BY populationpercentagepopulationinfected desc ;
-- lets break down by contentes

 -- showing contintents with the highest death count per population 
 SELECT 
   continent,
   max(cast (total_deaths as int ))as highest_death 
FROM CovidDIE
where continent is not  null 
group by continent 
ORDER BY highest_death desc ;

-- global number 

SELECT 
    date,
    sum(new_cases)as total_cases ,
    sum(cast (new_deaths as int))as total_deaths,
    (sum(cast (new_deaths as int)) / sum(new_cases)) * 100 AS deathpercentage
FROM 
    CovidDIE
where continent is not  null 
group by date 
ORDER BY 1, 2;

---------------------
-- looking total population vs vaccinations
with popvsvec(continent,location,date,population,new_vaccinations,rollingpepolevaccinations)
as
(
SELECT
    di.continent,
    di.location,
    di.date,
    di.population,
    va.new_vaccinations,
    SUM(CAST(va.new_vaccinations AS INT)) OVER (PARTITION BY di.location order by di.location, di.date  ) AS rollingpepolevaccinations
FROM
    CovidDIE AS di
JOIN
    covidvaccinations AS va ON di.location = va.location AND di.date = va.date
WHERE di.continent IS NOT NULL

)
select *,(rollingpepolevaccinations/population)*100
from popvsvec 



-- table

drop table if exists pepole_percenatge_vaccinations
create table pepole_percenatge_vaccinations
(   continent varchar(255),
    location varchar(255),
    date date,
    population numeric,
    new_vaccinations numeric,
    rollingpepolevaccinations numeric
)

insert into pepole_percenatge_vaccinations
SELECT
    di.continent,
    di.location,
    di.date,
    di.population,
    va.new_vaccinations,
    SUM(CAST(va.new_vaccinations AS INT)) OVER (PARTITION BY di.location order by di.location, di.date  ) AS rollingpepolevaccinations
FROM
    CovidDIE AS di
JOIN
    covidvaccinations AS va ON di.location = va.location AND di.date = va.date
--WHERE di.continent IS NOT NULL

select *,(rollingpepolevaccinations/population)*100
from pepole_percenatge_vaccinations 

--- crating view to store date for later visulizations

create view pepole_percenatge_vaccinations as

sELECT
    di.continent,
    di.location,
    di.date,
    di.population,
    va.new_vaccinations,
    SUM(CAST(va.new_vaccinations AS INT)) OVER (PARTITION BY di.location order by di.location, di.date  ) AS rollingpepolevaccinations
FROM
    CovidDIE AS di
JOIN
    covidvaccinations AS va ON di.location = va.location AND di.date = va.date
WHERE di.continent IS NOT NULL
--order by 2,3
 select * from  pepole_percenatge_vaccinations







