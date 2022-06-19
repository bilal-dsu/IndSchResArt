__author__ = "Bilal Hayat Butt"

__license__ = """The MIT License (MIT)

Copyright (c) [2021] [Bilal Hayat Butt]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."""

__version__ = "1.0.1"

import json
import requests
import os, sys
import pandas as pd
import glob
import pickle
from dask.distributed import Client, progress
import dask.dataframe as dd

def getDOIsFromList (DOIList, DOIFile):
   with open(DOIFile, 'wb') as f:
            pickle.dump(DOIList, f, pickle.HIGHEST_PROTOCOL)
            
def getDOIsFromCSV (CSVFile, refDf, DOIColumn):
    DF = pd.read_csv(CSVFile)
    DF = DF.rename(columns = {DOIColumn:'cited'})
    DF.to_pickle(refDf)
    del DF

def fetchMetadataFromCrossrefAPI (path_to_initial_dump, ISSNarg, End_Date):
    ISSNList = ISSNarg.split(",")
    #getting current directory and dumping data in that directory

    if not os.path.exists(path_to_initial_dump): #check if folder already exist, if it doesn't then create it
        os.makedirs(path_to_initial_dump)
    #print (ISSNList)

    for ISSN in ISSNList:
        offset=0

        #Crossref API Call
        API_String = 'http://api.crossref.org/works?filter=issn:'+ISSN+',until-pub-date:'+End_Date
        response = requests.get(API_String)
        data = response.json()

        # No of Records in Journal
        Records = data['message']['total-results']
        Req_count = (Records / 1000) + 1
        #print ("\n",ISSN, Records, Req_count,"\n")
        # Save data in Json files

        for Request in range(int(Req_count)):
                response = requests.get(API_String+'&rows=1000'+'&offset='+str(offset))
                data = response.json()
                print (ISSN, offset, data['message']['total-results'])
                newpathforDataDump = os.path.join(path_to_initial_dump,'Metadata'+ISSN+'_'+str(Request)+'.json')
                with open(newpathforDataDump, 'w') as f1:
                        json.dump(data , f1)
                        offset = offset + 1000

def getDOIsFromJSON (path_to_json, DOIFile):
    pd.set_option('display.max_columns', None)
    file_list = glob.glob(path_to_json)

    DOIs = []
    for file in file_list:
        data = pd.read_json(file, lines=True) # read data frame from json file
        for records in data['message']:
            for item in records['items']:
                DOIs.append(item['DOI'])
    with open(DOIFile, 'wb') as f:
        pickle.dump(DOIs, f, pickle.HIGHEST_PROTOCOL)

def getDOIsFromRef (path_to_COCI, selectCOCICols, physicalCores, virtualPerCore, refDOI, refDf):
    
    client = Client(n_workers=physicalCores, threads_per_worker=virtualPerCore, memory_limit=0)
    print(client) 
    # Ref: https://examples.dask.org/dataframe.html
    ddf = dd.read_csv(path_to_COCI, usecols=selectCOCICols, dtype={'citing': 'string', 'cited': 'string', 'creation': 'object', 'oci': 'string'})
    ddf['creation'] = dd.to_datetime(ddf['creation'], errors = 'coerce')
    
    ddf = ddf[ddf.oci.str.startswith('020')]#use only Crossref DOI from COCI
    
    DOI_ddf = ddf[ddf['citing']==refDOI]
    refDF = DOI_ddf.compute()  
    
    refDF.to_pickle(refDf)
    client.close()

def splitDOIs (refDf, DOIPkl, DOItestPkl, metaData, sample_n, sample_random_state):
  
    refDF = pd.read_pickle(refDf)
    
    DF = pd.read_pickle(metaData)

    print (len(refDF.cited.unique()))
    DF = DF.set_index('DOI')
    refDF = refDF[refDF.cited.isin(DF.index)]
    print (len(refDF.cited.unique()))
    
    #https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sample.html
    train=refDF.sample(n=sample_n,random_state=sample_random_state) #random state is a seed value
    test=refDF.drop(train.index)
    
    with open(DOIPkl, 'wb') as f:
        pickle.dump(list(train.cited.unique()), f, pickle.HIGHEST_PROTOCOL)
    
    with open(DOItestPkl, 'wb') as f:
        pickle.dump(list(test.cited.unique()), f, pickle.HIGHEST_PROTOCOL)
    

def filterCOCIDump (path_to_COCI_parquet, physicalCores, virtualPerCore,citNetFull, DOIPkl, totalLevels):
    client = Client(n_workers=physicalCores, threads_per_worker=virtualPerCore)
    print(client)
    ddf = dd.read_parquet(path_to_COCI_parquet)     
    with open(DOIPkl, 'rb') as f:
        DOIList = pickle.load(f)
    print ("Ego Nodes Edges")
    for lvl in range(0,totalLevels):
        DOI_ddf = ddf[(ddf['citing'].isin(DOIList)) | (ddf['cited'].isin(DOIList))]            
        res_df = DOI_ddf.compute()   
        res_df.to_pickle(citNetFull)

        DOIList = list(res_df['citing'].unique()) #convert df column to list
        DOIList.extend(list(res_df['cited'].unique())) #merge both df columns as list
        DOIList = list(set(DOIList)) #get unique DOIs in list
        print(lvl+1, len(DOIList), len(res_df.index))
    client.close()

        
def filterMetadata(metadataFull, citNetFull, metaData, citNet):
    COCI = pd.read_pickle(citNetFull)
    df = pd.read_pickle(metadataFull)
    df = df.set_index('DOI')

    artList = list(COCI.citing.unique())
    df = df[df.index.isin(artList)]
    artList = df.index.unique().tolist()
    COCI = COCI[(COCI.citing.isin(artList)) & (COCI.cited.isin(artList))]

    COCI['bidir'] = COCI.apply(lambda row: ''.join(sorted([row['citing'], row['cited']])), axis=1)
    COCI = COCI.drop_duplicates('bidir').drop(['bidir'], axis=1)

    df.to_pickle(metaData)
    COCI.to_pickle(citNet)
    del df
    del COCI
    
def filterZeroOutDegNodesFromCOCI (metaData, citNet):
    COCI = pd.read_pickle(citNet)
    df = pd.read_pickle(metaData)
    
    prevCnt = 0
    nextCnt = COCI.citing.nunique()
    while(prevCnt!=nextCnt):
        artList = list(COCI.citing.unique())
        COCI = COCI[COCI.cited.isin(artList)]
        prevCnt = nextCnt
        nextCnt = COCI.citing.nunique()      

    df = df[df.index.isin(artList)]
    
    tempdf = df.drop(['Authors', 'Year'], axis=1)
    COCI = COCI.merge(tempdf, left_on='citing', right_index=True)
    COCI = COCI.merge(tempdf, left_on='cited', right_index=True, suffixes=('_citing', '_cited'))

    prevCnt = 0
    nextCnt = COCI.Venue_citing.nunique()
    while(prevCnt!=nextCnt):
        jnlList = list(COCI.Venue_citing.unique())
        COCI = COCI[COCI.Venue_cited.isin(jnlList)]
        prevCnt = nextCnt
        nextCnt = COCI.Venue_citing.nunique()      

    df = df[df.Venue.isin(jnlList)]

    df.to_pickle(metaData)
    COCI.to_pickle(citNet)
    del df
    del COCI

def generateTemporalNetwork(metaData,RankYearStart,RankYearEnd, CutOffStart, CutOffEnd, metaDataRankYear, metaDataCutOffYear, ArticleHash, ArticleGraph):

    graphFile = snap.TFIn(ArticleGraph)
    graph = snap.TNEANet.Load(graphFile)
    hashes = snap.TFIn(ArticleHash)
    mapping = snap.TStrIntSH (hashes)

    lst = []
    for N in graph.Nodes():
        doi = mapping.GetKey(N.GetId())
        lst.append(doi)

    df = pd.read_pickle(metaData)
    df = df[df.index.isin(lst)]
    df = df[(df.Year.astype(int) >= CutOffStart) & (df.Year.astype(int)<= CutOffEnd)]
    df.to_pickle(metaDataCutOffYear)
    
    df = df[(df.Year.astype(int)>= RankYearStart) & (df.Year.astype(int)<= RankYearEnd)] 
    df.to_pickle(metaDataRankYear)  

    print("Metadata created %s & %s"% (metaDataRankYear, metaDataCutOffYear)) #To make sure file is created
