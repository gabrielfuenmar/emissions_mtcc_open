"""
Emissions training


Spatial functions
___________________
wkt_load - converts wkt in dataframe to geometry object.

Gabriel Fuentes: NHH (Norway)/ MTCC LatinAmerica: gabriel.fuentes@nhh.no



"""
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def wkt_load(
    spark: SparkSession,
    df: DataFrame, 
    wkt_field = 'WKT_geom',
    drop_wkt = True )-> DataFrame:
    '''
    Implement Sedona to make a data frame with a geometary object
    https://sedona.apache.org/tutorial/core-python/ 
    create a temp view that implements ST_GeomFromWKT() function.
    
    Tabular data that contains a field that has a wkt field that is going to be used to
    create a geometry object with field name geom and source string removed as no longer needed
    once geometry is created from it.
    
    Script from Justin McGurk at the ML group and refered to CSO-Ireland (_utility.cso_wkt_load())
    and modified for use in this project.

    Parameters
    ----------
    spark: SparkSession

    df: spark data frame

    wkt_field: String: Field name in df that contain wkt geometry string used for geometry creation.
        Removed from output by default
        default: WKT_geom
    
    drop_wkt: Boolean: Gives user option to drop wkt_field once consumed in geometry creation.
        default: True --> Field wkt_field will be removed from output
        False --> Field wkt_field will NOT be removed from output
    
    Returns
    -------
    spark data frame
        geometry object created in to new field: geom
    '''
    try:
        df.createOrReplaceTempView("x")
        geom_df= spark.sql(
        rf'''
        SELECT 
        *, 
        ST_GeomFromWKT({wkt_field}) as geom  
        
        FROM x

        '''
        )

        if drop_wkt:
            geom_df = geom_df.drop(rf'{wkt_field}')
        
        geom_df.printSchema()
        print(geom_df.head(1))  #Use this to throw error for bad geometry types
        
        return geom_df
    
    except Exception as e:
        print(f'Function requires pyspark.sql.dataframe.DataFrame object as input \
        \nCheck wkt field is well formed? \
        \nCheck field choice is wkt data?')
        print(f'Error: \n{e}\n')


