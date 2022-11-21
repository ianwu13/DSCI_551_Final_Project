import commands as cmd ##import commands script
import pandas as pd
import io

def mapPartition(file_name:str,part_name:str):
    read_part = cmd.readPartition(file_name,part_name)
    df = pd.read_csv(io.StringIO(read_part))
    return df

## Input fossil fuel file name and specify range for total fossil fuel and output years that fits the range
def find_year_within_fossil_range(file_name:str,lower:int,upper:int):
    fossil_locations = cmd.getPartitionLocations(file_name)
    parts =[fossil_locations]
    parts = parts[0].split("\n")
    results = []
    for i in parts:
        part = mapPartition(file_name,i)
        part = part[(part['Total'] >= lower) & (part['Total'] <= upper)]
        found = part["Year"].to_list()
        results.extend(found)
    return results

## Find sea level based on year and month
def find_sea_level_uncertainty(file_name:str,year:str,month:str):
    fossil_locations = cmd.getPartitionLocations(file_name)
    parts =[fossil_locations]
    parts = parts[0].split("\n")
    results = []
    for i in parts:
        df = mapPartition(file_name,i)
        df['year'] = [i.split("-")[0] for i in df["Time"]]
        df['month'] = [i.split("-")[1] for i in df["Time"]]
        found = df[(df['year'] == year) & (df['month'] == month)]
        found = found["GMSL uncertainty"]
        results.extend(found)
    return results

## Find year within range of co2 outputs
def find_year_within_co2_range(file_name:str,lower:int,upper:int):
    fossil_locations = cmd.getPartitionLocations(file_name)
    parts =[fossil_locations]
    parts = parts[0].split("\n")
    results = []
    for i in parts:
        part = mapPartition(file_name,i)
        part['year'] = [i.split("-")[0] for i in part["Date"]]
        part = part[(part['Average'] >= lower) & (part['Average'] <= upper)]
        found = part["year"].to_list()
        results.extend(found)
        results = list(set(results))
    return results

# Analytic function: find_year_within_fossil_range
# Select year 
# From fossil_fuel
# Where total >= lower and total <= upper

# In this case, mapPartiton(p) may take fossil_fuel in partition p, output years with different levels of fossil fuels usage. 
# Reduce function then identifies and returns the years with fossil fuel usage level that fits the specified value range.

# Analytic function: find_sea_level_uncertainty 
# Select ‘GMSL_uncertainty’
# From global_mean_sea_level
# Where month = ‘07’ and year = ‘2020’

# In this case, mapPartiton(p) may take find_sea_level_uncertainty in partition p, output the sea level uncertainty level by month and year. 
# Reduce function extracts month and year information from the ‘Time’ column to create columns ‘month’ and ‘year’, 
# then identifies and returns the uncertainty of sea level that fits the specified month and year values.

# Analytic function: find_year_within_co2_range
# Select year 
# From co2_ppm
# Where average >= lower and average <= upper

# In this case, mapPartiton(p) may take co2_ppm in partition p, output dates with different levels of average co2 emissions. 
# Reduce function extracts the year information from ‘Date’ to create the ‘year’ column, 
# then identifies and returns the years with average co2 emission level that fits the specified value range.






# Find the mean of global temperature of all years from two sources 
def find_mean_temp_each_source(file_name:str,group:str):
    global_temp_locations = cmd.getPartitionLocations(file_name)
    parts =[global_temp_locations]
    parts = parts[0].split("\n")
    results = []
    for i in parts:
        part = mapPartition(file_name,i)
        part['mean temp'] = part.groupby(group)['Mean'].mean()
        found = part["mean temp"].to_list()
        results.extend(found)
        results = list(set(results))
    return results


# create a new column for finding the absolute change of mean mass balance between every two years
def find_difference_two_years_glacier_mass(file_name:str):
    global_temp_locations = cmd.getPartitionLocations(file_name)
    parts =[global_temp_locations]
    parts = parts[0].split("\n")
    results = []
    for i in parts:
        part = mapPartition(file_name,i)
        part['difference'] = part['Mean cumulative mass balance'].diff()
        found = part["difference"].to_list()
        results.extend(found)
        results = list(set(results))
    return results



# Find years that the number of observation for mass balance of glacier is between certain range
def find_year_within_observation_range(file_name:str,lower:int,upper:int):
    glacier_locations = cmd.getPartitionLocations(file_name)
    parts =[glacier_locations]
    parts = parts[0].split("\n")
    results = []
    for i in parts:
        part = mapPartition(file_name,i)
        part = part[(part['Number of observations'] >= lower) & (part['Number of observations'] <= upper)]
        found = part["Year"].to_list()
        results.extend(found)
        results = list(set(results))
    return results
