exp_headers = [
    # find_year_within_fossil_range
    'SELECT year</br>FROM fossil_fuels.csv</br>WHERE total >= lower AND total <= upper;',
    # find_sea_level_uncertainty
    'SELECT `GMSL_uncertainty`</br>FROM global_mean_sea_level.csv</br>WHERE month = month AND year = year;',
    # find_year_within_co2_range
    'SELECT year</br>FROM co2_ppm.csv</br>WHERE average >= lower AND average <= average;',
    # diff_fuel_within_temp_range
    'SELECT ff.Year, ff.`Gas Fuel`, ff.`Liquid Fuel`, ff.`Solid Fuel`, gt.Mean</br>FROM fossil_fuels.csv ff</br>LEFT JOIN global_temp.csv gt ON ff.Year = gt.Year</br>WHERE gt.Mean >= temp;'
    # co2_glacier_within_year_range
    'SELECT YEAR(co.Date) AS Year, gl.`Mean cumulative mass balance`, co.Average</br>FROM glaciers.csv gl</br>LEFT JOIN co2_ppm.csv co ON gl.Year = co.Year</br>WHERE Year >= start_year AND Year <= end_year;'

]

exp_bodies = [
    'In this case, mapPartiton(p) may take fossil_fuel in partition p, output years with different levels of fossil fuels usage. Reduce function then identifies and returns the years with fossil fuel usage level that fits the specified value range.',
    'In this case, mapPartiton(p) may take find_sea_level_uncertainty in partition p, output the sea level uncertainty level by month and year. Reduce function extracts month and year information from the ‘Time’ column to create columns ‘month’ and ‘year’, then identifies and returns the uncertainty of sea level that fits the specified month and year values.',
    'In this case, mapPartiton(p) may take co2_ppm in partition p, output dates with different levels of average co2 emissions. Reduce function extracts the year information from ‘Date’ to create the ‘year’ column, then identifies and returns the years with average co2 emission level that fits the specified value range.',
    'In this case, mapPartiton(p) may take fossil_fuel and global_temp in partition p, output different types of fuel and the year when global temperature is greater than a certain value. Reduce function then identifies and returns the years and amount of gas, liquid and solid fuel when the global temperature is greater than a specified value.',
    'In this case, mapPartiton(p) may take glaciers and co2_ppm in partition p, output dates with different levels of average co2 emissions and mean cumulative mass balance of glaciers. Reduce function extracts the year information from ‘Date’ to create the ‘Year’ column, then identifies and returns the average co2 emission level and mean cumulative mass balance of glaciers within a specified year range.',
]

funct_forms = [
    #''.join([line.rstrip('\n') for line in open('pmr/forms/example_form.html', 'r').readlines()]),
    'Parameters are a lower bound INT value and upper bound INT value for total carbon emission from fossil fuel',
    'Parameters are an INT value for month and an INT value for year',
    'Parameters are a lower bound FLOAT value and upper bound FLOAT value for monthly mean CO2',
    'Parameter is a FLOAT value for average global mean temperature',
    'Parameters are an INT value for start year and an INT value for end year',
]