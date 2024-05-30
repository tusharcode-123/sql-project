select *
from coviddeaths
where continent is not null
order by 3,4;

select *
from covidvactination
order by 3,4;

select location,`date`,total_cases,new_cases,total_deaths,population
from coviddeaths
order by 1,2;

select count(total_cases),count(total_deaths) 
from coviddeaths;

select location,`date`,total_cases,new_cases,total_deaths,(total_deaths/total_cases)*100 as deathration
from coviddeaths
order by 1,2;


select location,population,max(total_cases),max((total_cases/population))*100 as percentpopulationinfected
from coviddeaths
group by location,population
order by percentpopulationinfected DESC;

-- continent with higher death rate per population
select continent,MAX(cast(total_deaths as UNSIGNED)) as Totaldeath
from coviddeaths
where continent is not null
group by continent
order by totaldeath DESC;

-- global number
select sum(cast(new_cases as unsigned)) as total_cases,
sum(cast(new_deaths as unsigned)) as total_deaths,sum(cast(new_deaths as unsigned))/sum(total_cases)*100 as deathration
from coviddeaths
order by 1,2;


select * from covidvactination;

select d.continent,d.location,d.`date`,d.population,vac.new_vaccinations,
sum(cast(vac.new_vaccinations as unsigned)) over (partition by d.location order by d.location,d.`date`) as rollingpeoplevaccinations
from coviddeaths d 
join covidvactination vac on d.location = vac.location 
and d.`date` = vac.`date` 
where d.continent is not null
order by 2,3;

with popvsvac (continent,location,`date`,population,new_vaccinations,rollingpeoplevaccinations) as
 (
 select d.continent,d.location,d.`date`,d.population,vac.new_vaccinations,
sum(cast(vac.new_vaccinations as unsigned)) over (partition by d.location order by d.location,d.`date`) as rollingpeoplevaccinations
from coviddeaths d 
join covidvactination vac on d.location = vac.location 
and d.`date` = vac.`date` 
where d.continent is not null
-- order by 2,3
)
select *,
(rollingpeoplevaccinations/population) * 100 from popvsvac;









