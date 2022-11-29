from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
import pyspark.sql.functions as fc
from pyspark.sql.window import Window

mn_private_ip = ''

spark = SparkSession.builder.appName("551_App").master(f"spark://{mn_private_ip}:7077")\
    .config("spark.driver.host", mn_private_ip)\
    .config('spark.driver.bindAddress', '0.0.0.0')\
    .config("spark.dynamicAllocation.enabled", 'false')\
    .getOrCreate()
    # .config("spark.executor.memory", "512m")\
    # .config("spark.shuffle.service.enabled", "false")\
    # .config("spark.dynamicAllocation.enabled", "false")\

co2 = spark.read.json('co2_ppm.csv')
glaciers = spark.read.json('glaciers_csv.csv')
temp = spark.read.json('global_temp.csv')
ff = spark.read.json('fossil_fuels.csv')
sl = spark.read.json('global_mean_sea_level.csv')


# diff_fuel_within_temp_range
def fun_4(params):
    global glaciers, temp

    tmp_temp = temp.where(temp.Source == 'GISTEMP')

    res = glaciers.join(tmp_temp, glaciers.Year == tmp_temp.Year).select('Mean avg_num_anomalies', 'Mean cumulative mass balance')

    df = res.toPandas()
    m_anom = list(df['Mean avg_num_anomalies'])
    masses = list(df['Mean cumulative mass balance'])

    return [(i, j) for i, j in zip(m_anom, masses)]


# average_co2_ppm_by_month
def fun_3(params):
    global co2

    tmp = co2.select(co2.date_format('Date','yyyy-MM-dd').alias('month'), 'Average').groupby('month').avg()
    df = tmp.select('month', 'Average').toPandas()
    m = list(df['Average'])
    a = list(df['Mean cumulative mass balance'])

    return [(i, j) for i, j in zip(m, a)]


# find_years_within_co2_range
def fun_2(params):
    global co2

    res = co2.where(co2.Average < params[1]).where(co2.Average > params[0]).select('Year')

    df = res.toPandas()
    years = list(df['Year'])

    return [(i, 1) for i in years]


# find_sea_level_for_date
def fun_1(params):
    global sl

    res = sl.where(sl.Month == params[0]).where(sl.Year == params[1]).select('GMSL')

    df = res.toPandas()
    gmsl = list(df['GMSL'])

    return [(i, 1) for i in gmsl]


# find_year_within_fossil_fuel_use_range
def fun_0(params):
    global ff

    res = ff.where(ff.Total > params[0]).where(ff.Total < params[1]).select('year')

    df = res.toPandas()
    years = list(df['year'])

    return [(i, 1) for i in years]


funct_guide = [fun_0, fun_1, fun_2, fun_3, fun_4]


def pmr_wrapper(imp: str, funct_id: int, params: list):
    return funct_guide[funct_id](params)


def call_funct(data: dict):
    imp = data['imp']
    funct_id = int(data['funct'])
    params = data['params'].split('\n')

    res = pmr_wrapper(imp, funct_id, params)
    return {'final_res': res}
