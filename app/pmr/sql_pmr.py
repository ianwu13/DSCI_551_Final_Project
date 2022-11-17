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