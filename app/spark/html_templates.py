exp_headers = [
    # find_year_within_fossil_fuel_use_range
    'SELECT year</br>FROM fossil_fuels.csv</br>WHERE Total >= lower AND Total <= upper;',
    # find_sea_level_for_date
    'SELECT GMSL</br>FROM global_mean_sea_level.csv</br>WHERE Month = month AND Year = year;',
    # find_years_within_co2_range
    'SELECT count(*)</br>FROM co2_ppm.csv</br>WHERE Average >= lower AND Average <= upper;',
    # average_co2_ppm_by_month
    'SELECT MONTH(co.Date), AVG(co.Average)</br>FROM co2_ppm.csv co</br>GROUP BY MONTH(co.date);',
    # diff_fuel_within_temp_range
    'SELECT gt.Mean avg_num_anomalies, gc.`Mean cumulative mass balance`</br>FROM glaciers_csv.csv gc</br>INNER JOIN global_temp.csv gt </br>ON gc.Year = gt.Year</br>WHERE gt.Source = "GISTEMP"'
]

exp_bodies = [
    'In this case, mapPartiton(p) takes fossil_fuel in partition p, output years with valid levels of fossil fuels usage.</br></br>Reduce function is not needed in this case, but the it used to combine results into a single list.</br></br>Note: Total ranges from 3 to 9167.',
    'In this case, mapPartiton(p) takes find_sea_level_uncertainty in partition p, output the global mean sea level for valid months and years.</br></br>Reduce function is not needed, but is used to combine results from different partitions.</br></br>Note: Year ranges from 1880 to 2013.',
    'In this case, mapPartiton(p) takes co2_ppm in partition p, output dates with valid levels of average co2 emissions.</br></br>Reduce function combines these results and obtains the total count.</br></br>Note: Average co2_ppm ranges from 0 to 411.24, -99.99 is used to indicate missing data',
    'In this case, mapPartiton(p) takes co2_ppm in partition p, outputs key value pairs corresponding to each month of the year for the keys, and with the average co2 ppm as the value.</br></br>Reduce function combines these key value pairs by key, then identifies and returns the average co2 ppm level for each month.',
    'In this case, mapPartiton(p) takes glaciers.csv and global_temp in each partition p, outputs year as the key for each, with a second key value pair as the value. The key for the second kv pair is the file it comes from, and the value is the respective column being searched for in the file. Map also handle filterint the results based on the gt Source value.</br></br>Reduce function then combines these kv pairs based on the year key, and returns the resultant num_anomalies-mass_distribution pairs.'
]

form_names = [
    'find_year_within_fossil_range.html', # 'Parameters are a lower bound INT value and upper bound INT value for total carbon emission from fossil fuel',
    'find_sea_level.html', # 'Parameters are an INT value for month and an INT value for year',
    'find_year_within_co2_range.html', # 'Parameters are a lower bound FLOAT value and upper bound FLOAT value for monthly mean CO2',
    'average_co2_ppm_by_month.html', # Empty form since funciton needs no parameters
    'glaciers_temp.html' # 'Parameter is a FLOAT value for average global mean temperature',
]

funct_forms = [''.join([line.rstrip('\n') for line in open(f'spark/forms/{f}', 'r').readlines()]) for f in form_names]

