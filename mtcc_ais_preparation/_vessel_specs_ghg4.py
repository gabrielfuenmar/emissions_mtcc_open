"""
Emissions training


Standard vessel_specs_adjustnment
___________________
clean_string - removes symbols from reported vessel type

compare_similarity - Get the closest standardized vessel type from their cosine_similarity. 
                  Similarity > 75% is a match.

bin_finder - Classifies vessel types on their respective bin as recognized in the IMO GHG4 report method.
Table 8 pp.48.

adapted_specs_imo - adjuste the vessel specs online with the IMO GHG4 report methods. 

Gabriel Fuentes: NHH (Norway)/ MTCC LatinAmerica: gabriel.fuentes@nhh.no
"""


import pandas as pd
import numpy as np
import string
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
import requests
from io import StringIO
import importlib.resources as pkg_resources

def find_folder_csv(folder_name,file_name):
    """
    Finds csv within instructed folder and loads a Pandas df
    
    Args:
        folder name (str): The input text as folder name.
        file_name (str): The input text as file name, including extension .csv.

    Returns:
        DataFrame: A dataframe with information stored at the package csv
    """


    try:
        with pkg_resources.files("mtcc_ais_preparation.{}".format(folder_name)).joinpath(file_name).open() as f:
            df = pd.read_csv(f)
            return df

    except Exception as e:
        print(f'Have you typed in the correct folder and file name. File name should include extension i.e. .csv')
        print(f'Error: \n{e}\n') 


##Map Vessel_type
map_vessel_type = find_folder_csv("vessel_types_adjust","map_vessel_type_imo4.csv")

#Types Table
vessel_type=find_folder_csv("vessel_types_adjust","type_table.csv")
#####Functions--------------------
##Cleaning punctuations from new ais_types
def clean_string(text):
    """
    Remove punctuation from the input text and convert it to lower case.
    
    Args:
        text (str): The input text.

    Returns:
        str: The cleaned text.
    """
    text=''.join([word for word in text if word not in string.punctuation])
    text=text.lower()
    return text

##base shiptypelevel5 to comapre to
base_stype=map_vessel_type.ShiptypeLevel5.unique().tolist()

##Compare similiraty higher than 75% and return respective shiptype5 value
def compare_similarity(text):
    """
    Compare the similarity of the input text with a list of base ship types.
    If the similarity is higher than 75%, return the respective ship type.
    Otherwise, return None.
    
    Args:
        text (str): The input text.

    Returns:
        str: The matching ship type, or None if not found.
    """
    comp=[text]+base_stype
    cleaned=list(map(clean_string,comp))
    vectors=CountVectorizer().fit_transform(cleaned)
    vectors=vectors.toarray()
    csim=cosine_similarity(vectors)
    
    val_com=np.max(csim[0,1:])
    
    if val_com>0.75:
        v_type=base_stype[np.argmax(csim[0,1:])]
    else:
        v_type=None
    
    return v_type   

##Imo bin finder
def bin_finder(vessel_t,value,df_in):
    """
    Find the IMO bin for the given vessel type and value.
    
    Args:
        vessel_t (str): The vessel type.
        value (float): The value to classify.
        df_in (pd.DataFrame): The input DataFrame with bin ranges.

    Returns:
        int: The IMO bin, or 0 if not found.
    """
    try:
        bin_imo=df_in[((df_in.StandardVesselType==vessel_t)&(df_in.mindiff<=value)&(df_in.maxdiff>=value))].imo4bin.iloc[0]
    except:
        bin_imo=0
    return bin_imo
###++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
###Unit of cargo measurement per vessel type
unit={'Bulk carrier':'Deadweight',
     'Chemical tanker':'Deadweight',
     'Container':"TEU",
     'General cargo':'Deadweight',
     'Liquified gas tanker':'GrossTonnage',
     'Oil tanker':'Deadweight',
     'Other liquids tankers':'Deadweight',
     'Ferry-pax only':'GrossTonnage',
     'Cruise':'GrossTonnage',
     'Ferry-RoPax':'GrossTonnage',
     'Refrigerated bulk':'Deadweight',
     'Ro-Ro':'Deadweight',
     'Vehicle':'GrossTonnage',
     'Yacht':'GrossTonnage',
     'Service-tug':'GrossTonnage',
     'Miscellaneous-fishing':'GrossTonnage',
     'Offshore':'GrossTonnage',
     'Service-other':'GrossTonnage',
     'Miscellaneous-other':'GrossTonnage'}

##Engine type allocation
oil_eng=['Diesel-Elec & Gas Turbine(s)','Oil Engs & Fuel Cell-Electric''Oil Eng(s), Elec-Dr, Aux Sail','Oil Engines, Geared & Elec. Dr','Oil Eng(s) & Gas Turb(s) El.Dr','Oil Eng(s) Direct Dr, Aux Sail','Oil Eng(s) Dd & Gas Turb(s) El','Oil Engines, F&S, Geared Drive','Oil Engines, Direct & Elec. Dr','Oil Engines, Elec. & Direct Dr','Oil Engine(s), Drive Unknown','Oil Engines, Elec. & Geared Dr','Oil Eng(s), Geared, Aux Sail','Oil Engs & Gas Turb(s), Geared','Oil Engine(s), Electric Drive','Oil Engine(s), Direct Drive','Oil Engine(s), Geared Drive']
sail=['Sail, Aux Petrol Eng(s) D.Dr.','Sail, Aux Oil Eng(s), Elec Dr.','Sail, Aux Oil Eng(s), Geared','Sail','Sail, Aux Oil Eng(s) Direct-Dr',]
gas_tur=['Gas Turbine(s), Electric Drive','Gas Turbine(s) Or Diesel Elec.','Gas Turbine(s) & Diesel Elec.','Gas Turbine(s), Geared Drive',]
steam=['S.Turb, Gear & Oil Eng(s), Ele','St. Turb(s) Elec Dr. Or D.E.','Steam Turbine(s), Direct Drive','Steam Recip(s) With Lp Turbine','Steam Turbine(s), Elec.Drive','Steam- & Gas-Turbines, Geared','Steam Turbine(s), Geared Drive','Steam Recip(s), Direct Drive',]


def adapted_specs_imo(df_unique_imo):
    """
    Input (IMPORTANT): AIS Df with unique IMO and merged with IHS Markit Specs.
    Rules adapted to IMO GHG 4 study. Important to merge with AIS sequences
    Output: Vessel Specs of vessels in AIS records with appropiate IMO GHG4 categories
    ''
    Convert the vessel specs to the standard setup as per the IMO GHG4.
    If a vessel type is null as per the Lloyds Fleet Register, then a cosine similiary is
    conducted to find the closest match from a pool of options.
  
    
    Script from Gabriel Fuentes and the MTCC LatinAmerica

    Parameters
    ----------
    df_unique_imo: Pandas DataFrame!!!

        
    Returns
    -------
    Pandas dataframe
        with Lloyds register information plus new columns ('imobin', 'fuel', 'meType')
    """
    try:
    
        df_unique_imo.rename(columns={"vessel_type_main":"ais_type","length":"ais_loa","width":"ais_beam"},inplace=True)
        
        ind=df_unique_imo.copy()
        
        ind=ind.assign(ShiptypeLevel5=np.where(ind.ShiptypeLevel5.isnull(),ind.ais_type,ind.ShiptypeLevel5))
        ##Remove values with Shiptypelevel5 null. Not much else to do with this records. 
        ##Remove nans before similarity check
        ind=ind[ind.ShiptypeLevel5.notnull()]
        ind=ind.assign(ShiptypeLevel5=np.where(ind.ShiptypeLevel5.isin(base_stype),ind.ShiptypeLevel5,
                                            ind.ShiptypeLevel5.apply(lambda x: compare_similarity(x))))
        ##Ensure no vessel without Standard vessel type
        ind=ind[ind.ShiptypeLevel5.notnull()]

        ##---Pending----Inputation here input from AIS(Length,Beam) and Shiptypelevel5 to have [DWT,GT]. Potential RF Regressor (missForest).
        
        ind=pd.merge(ind,map_vessel_type,how="left",on='ShiptypeLevel5')
        
        ind=ind.assign(imobin=ind.apply(lambda x: bin_finder(x.StandardVesselType,x[unit[x.StandardVesselType]],vessel_type),axis=1))
        
        ###Fuel allocation
        ind=ind.assign(fuel=np.where(((ind.FuelType1First=='Residual Fuel')|(ind.FuelType2Second=='Residual Fuel')),
                                    np.where(((ind.PropulsionType.isin(['Steam Turbine(s), Geared Drive','S.Turb, Gear & Oil Eng(s), Ele','Steam Recip(s), Direct Drive','Steam- & Gas-Turbines, Geared','Steam Turbine(s), Elec.Drive','Steam Recip(s) With Lp Turbine','Steam Turbine(s), Direct Drive','St. Turb(s) Elec Dr. Or D.E.',]))\
                                                                    &(ind.StandardVesselType=='Liquified gas tanker')),"LNG","HFO"),
                                        np.where(((ind.FuelType1First=='Distillate Fuel')&(ind.FuelType2Second=='Distillate Fuel')),"MDO",
                                        np.where(((ind.FuelType1First=='Distillate Fuel')&(ind.FuelType2Second.isin(['Yes, But Type Not Known','Not Applicable','Unknown',None]))),"MDO",
                                        np.where(((ind.FuelType1First.isin(['Yes, But Type Not Known','Not Applicable','Unknown',None]))&(ind.FuelType2Second=='Distillate Fuel')),"MDO",
                                        np.where(((ind.FuelType1First=='Coal')&(ind.FuelType2Second=='Distillate Fuel')),"MDO",
                                        np.where(((ind.FuelType1First=='Methanol')&(ind.FuelType2Second=='Distillate Fuel')),'Methanol',
                                            np.where((((ind.FuelType1First=='Residual Fuel')|(ind.FuelType2Second=='Residual Fuel'))&\
                                                    ((ind.StandardVesselType=='Liquified gas tanker')&(ind.PropulsionType.isin(['Steam Turbine(s), Geared Drive','S.Turb, Gear & Oil Eng(s), Ele','Steam Recip(s), Direct Drive','Steam- & Gas-Turbines, Geared','Steam Turbine(s), Elec.Drive','Steam Recip(s) With Lp Turbine','Steam Turbine(s), Direct Drive','St. Turb(s) Elec Dr. Or D.E.',])))),'LNG',
                                            np.where(((ind.FuelType1First=='Gas Boil Off')&(ind.FuelType2Second=='Distillate Fuel')),'LNG',
                                            np.where(((ind.FuelType1First.isin(["LNG",'Lpg','Lng']))&(ind.FuelType2Second=='Distillate Fuel')),'LNG',
                                            np.where(((ind.FuelType1First.isin(["LNG",'Lpg','Lng']))&(ind.FuelType2Second.isin(['Yes, But Type Not Known','Not Applicable','Unknown',None]))),'LNG',
                                            np.where(((ind.FuelType1First.isin(['Yes, But Type Not Known','Not Applicable','Unknown',None]))&(ind.FuelType2Second.isin(["LNG",'Lpg','Lng']))),'LNG',      
                                            np.where(ind.FuelType2Second=='Gas Boil Off','LNG',
                                                np.where(((ind.FuelType1First=='Nuclear')&(ind.FuelType2Second=='Distillate Fuel')),'Nuclear',
                                                np.where(((ind.FuelType1First=='Nuclear')&(ind.FuelType2Second.isin(['Yes, But Type Not Known','Not Applicable','Unknown',None]))),'Nuclear',
                                                        np.where(((ind.FuelType1First=='Coal')&(ind.FuelType2Second.isin(['Yes, But Type Not Known','Not Applicable','Unknown',None]))),'Coal',
                                                                np.where(ind.FuelType1First=='Methanol','Methanol',                                              
                                    None))))))))))))))))
                    )


        ###Engine types
        ind=ind.assign(meType=np.where(ind.PropulsionType.isin(oil_eng),
                                    np.where(ind.MainEngineRPM<=300,"SSD",
                                    np.where(ind.MainEngineRPM.between(301,900),"MSD",
                                    np.where(ind.MainEngineRPM>900,"HSD","SSD"))),
                            np.where(ind.PropulsionType.isin(['Petrol Engine(s), Direct Drive','Petrol Engine(s), Geared Drive']),"HSD",       
                            np.where(ind.PropulsionType.isin(sail),"Sail",
                            np.where(ind.PropulsionType=='Battery-Electric',"Batteries",
                            np.where(ind.PropulsionType=='Non-Propelled','Non-Propelled', 
                            "SSD"))))))

        ind=ind.assign(meType=np.where(ind.fuel=="LNG",
                                    np.where(((ind.MainEngineModel.str.contains("X"))|(ind.MainEngineModel.str.contains("DF"))),"LNG-Otto-SS",
                                    np.where(ind.MainEngineRPM>300,"LNG-Otto-MS",    
                                    np.where(ind.MainEngineModel.str.contains("ME"),"LNG-Diesel","LNG-Otto-MS"                                       
                                    ))),
                                np.where(ind.fuel=="Methanol","Methanol", 
                                    ind.meType)))

                
        ##Gas turbines and Steam turbines conditional on former filters
        ind=ind.assign(meType=np.where(((ind.PropulsionType.isin(gas_tur))|(((ind.meType.isin(["SSD","MSD"]))&(ind.fuel=="Gas")))),"Gas Turbine",
                            np.where(ind.PropulsionType.isin(steam),"Steam Turbine",
                            ind.meType                      
                        ))
                    )
        ind=ind.assign(fuel=np.where(ind.meType=="Sail","Sail",
                            np.where(ind.meType=="Non-Propelled","Non-Propelled",
                            np.where(((ind.fuel.isnull())&(ind.meType=="HSD")),"MDO",
                            np.where(((ind.fuel.isnull())&(ind.meType=="MSD")),"MDO",
                            np.where(((ind.fuel.isnull())&(ind.meType=="SSD")),"HFO",
                                    ind.fuel)))))
                    )
        
        ind=ind.rename(columns={"Draught":"SummerDraught"})

        ind=ind[['imo','mmsi', 'GrossTonnage', 'Deadweight', 'LengthOverallLOA',
        'DateOfBuild', 'TEU', 'Powerkwmax', 'MainEngineModel', 'Speed', 'Speedmax', 'Speedservice', 'BreadthExtreme', 'SummerDraught', 'FuelType1Capacity',
        'FuelType2Capacity', 'LightDisplacementTonnage', 'MainEngineRPM', 'MainEngineType', 'Powerkwservice', 'PropulsionType',
        'ShiptypeLevel5', 'TotalBunkerCapacity', 'StandardVesselType', 'imobin', 'fuel', 'meType','ais_beam','ais_loa']]

        return ind

    except Exception as e:
        print(f'Have you passed the joined file of your ais sample and the Lloyds Register?   \
        Is you dataframe a Pandas dataframe. Transform your spark.Dataframe as spark.Dataframe.toPandas()')
        print(f'Error: \n{e}\n')