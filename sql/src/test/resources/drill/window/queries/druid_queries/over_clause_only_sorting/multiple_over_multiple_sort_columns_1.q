select
countryName, cityName,
row_number() over (order by countryName, cityName) as c1,
lag(cityName) over (order by cityName, countryName) as c2
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName
