exp_headers = [
    # find_year_within_fossil_range
    'SELECT year</br>FROM fossil_fuels.csv</br>WHERE total >= lower AND total <= upper;',
    # find_sea_level_uncertainty
    'SELECT `GMSL_uncertainty`</br>FROM global_mean_sea_level.csv</br>WHERE month = month AND year = year;',
    # find_year_within_co2_range
    'SELECT year</br>FROM co2_ppm.csv</br>WHERE average >= lower AND average <= upper;',
    # diff_fuel_within_temp_range
    'SELECT ff.Year, ff.`Gas Fuel`, ff.`Liquid Fuel`, ff.`Solid Fuel`, gt.Mean</br>FROM fossil_fuels.csv ff</br>LEFT JOIN global_temp.csv gt ON ff.Year = gt.Year</br>WHERE gt.Mean >= temp;',
    # average_co2_ppm_by_month
    'SELECT MONTH(co.Date) AS Month, AVG(co.), co.Average</br>FROM c.o2_ppm.csv co</br>GROUP BY MONTH(co.date);'
]

exp_bodies = [
    'In this case, mapPartiton(p) takes fossil_fuel in partition p, output years with different levels of fossil fuels usage. Reduce function then identifies and returns the years with fossil fuel usage level that fits the specified value range.',
    'In this case, mapPartiton(p) takes find_sea_level_uncertainty in partition p, output the sea level uncertainty level by month and year. Reduce function extracts month and year information from the ‘Time’ column to create columns ‘month’ and ‘year’, then identifies and returns the uncertainty of sea level that fits the specified month and year values.',
    'In this case, mapPartiton(p) takes co2_ppm in partition p, output dates with different levels of average co2 emissions. Reduce function extracts the year information from ‘Date’ to create the ‘year’ column, then identifies and returns the years with average co2 emission level that fits the specified value range.',
    'In this case, mapPartiton(p) takes fossil_fuel and global_temp in partition p, output different types of fuel and the year when global temperature is greater than a certain value. Reduce function then identifies and returns the years and amount of gas, liquid and solid fuel when the global temperature is greater than a specified value.',
    'In this case, mapPartiton(p) takes co2_ppm in partition p, outputs key value pairs corresponding to each month of the year. Reduce function combines these key value pairs, then identifies and returns the average co2 emission level for each month.'
]

form_names = [
    'find_year_within_fossil_range.html', # 'Parameters are a lower bound INT value and upper bound INT value for total carbon emission from fossil fuel',
    'find_sea_level_uncertainty.html', # 'Parameters are an INT value for month and an INT value for year',
    'find_year_within_co2_range.html', # 'Parameters are a lower bound FLOAT value and upper bound FLOAT value for monthly mean CO2',
    'diff_fuel_within_temp_range.html', # 'Parameter is a FLOAT value for average global mean temperature',
    'average_co2_ppm_by_month.html', # Empty form since funciton needs no parameters
]

funct_forms = [''.join([line.rstrip('\n') for line in open(f'pmr/forms/{f}', 'r').readlines()]) for f in form_names]

