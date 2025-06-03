select
countryName, cityName, added,
ROW_NUMBER() over () e2,
--lag(countryName) over (order by added) e5,
--count(*) over (partition by cityName order by countryName rows between 1 FOLLOWING and 3 FOLLOWING) c10,
count(*) over (partition by cityName order by countryName rows between current row and 1 following) c11
from wikipedia
where cityName in ('Vienna', 'Seoul')
group by countryName, cityName, added
