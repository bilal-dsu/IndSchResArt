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

import dask
import dask.bag as db
from dask.distributed import Client, progress
import dask.dataframe as dd
import pandas as pd
import json
import shutil


def flatten(record):
    Author_Name = ''
    if 'author' in record:
        for Author in record['author']:
            orcid=""
            if ('ORCID' in Author):
                orcid = Author['ORCID'][-19:]  
            if ('given' in Author) and (Author['given']!=''):
                if ('family' in Author) and (Author['family']!=''): 
                    a = Author['given'].split(" ")
                    Author_Name = Author_Name + ',' + orcid + ' '+ a[0] + ' ' + Author['family'].replace(" ", "-")
                else:
                    Author_Name = Author_Name + ',' + orcid + ' '+ Author['given'].replace(" ", "-")
            elif ('family' in Author) and (Author['family']!=''):
                Author_Name = Author_Name + ',' + orcid + ' '+ Author['family'].replace(" ", "-")
    titles = ''
    if 'title' in record:
        for i in record['title']:
            titles = titles + ',' + i
    
    ISSNs = ''
    if 'ISSN' in record:
        for i in record['ISSN']:
            ISSNs = ISSNs + ',' + i
            
    ISBNs = ''
    if 'ISBN' in record:
        for i in record['ISBN']:
            ISBNs = ISBNs + ',' + i
    
    SUBs = ''
    if 'subject' in record:
        for i in record['subject']:
            SUBs = SUBs + ',' + i

    Ctitles = ''
    if 'container-title' in record:
        for i in record['container-title']:
            Ctitles = Ctitles + ',' + i
        
    d = dict();
    if 'DOI' in record: d['DOI'] = record['DOI']
    else: d['DOI'] = ""
    d['Title'] = titles[1:]
    d['Venue'] = Ctitles[1:]
    if 'type' in record: d['Type'] = record['type']
    else: d['Type'] = ""
    d['Authors'] = Author_Name[1:]
    d['Subject'] = SUBs[1:]
    d['ISSN'] = ISSNs[1:]
    d['ISBN'] = ISBNs[1:]
    if 'published-online' in record: d['Year'] = str(record['published-online']['date-parts'][0][0])
    elif 'published-print' in record: d['Year'] = str(record['published-print']['date-parts'][0][0])
    else: d['Year'] = '1000'    
    return d

def convertCrossrefJSONDumpToParquetDDF(path_to_crossref, path_to_parquet, physicalCores, virtualPerCore, start_year, end_year):
    shutil.rmtree(path_to_parquet, ignore_errors=True)
    client = Client(n_workers=physicalCores, threads_per_worker=virtualPerCore)
    print(client)
    #Ref: https://examples.dask.org/bag.html
    b = db.read_text(path_to_crossref)\
    .filter(lambda x: len(x)>2)\
    .map(lambda x: x.replace('{"items":[','').replace(',\n',''))\
    .map(json.loads)\
    .map(flatten)\
    .filter(lambda record: ((record['Type'] == 'journal-article') | (record['Type'] == 'book-chapter') | (record['Type'] == 'proceedings-article')) & (int(record['Year']) >= start_year) & (int(record['Year']) <= end_year))

    ddf = b.to_dataframe()
    ddf.to_parquet(path_to_parquet)

    client.close()

def convertCOCIDumpToParquetDDF (path_to_COCI, selectCOCICols, physicalCores, virtualPerCore, path_to_COCI_parquet ,start_year, end_year):
    shutil.rmtree(path_to_COCI_parquet, ignore_errors=True)
    client = Client(n_workers=physicalCores, threads_per_worker=virtualPerCore, memory_limit=0)
    print(client) 
    # Ref: https://examples.dask.org/dataframe.html
    ddf = dd.read_csv(path_to_COCI, usecols=selectCOCICols, dtype={'citing': 'string', 'cited': 'string', 'creation': 'object', 'oci': 'string'})
    ddf['creation'] = dd.to_datetime(ddf['creation'], errors = 'coerce')
    
    ddf = ddf[(ddf['creation']>=str(start_year)) & (ddf['creation']<=str(end_year+1))]#integer year is transformed to date as Jan 1
    ddf = ddf[ddf.oci.str.startswith('020')]#use only Crossref DOI from COCI
    
    ddf = ddf.drop(['creation','oci'],axis=1)#drop columns not needed for edge list
    ddf.to_parquet(path_to_COCI_parquet)

    client.close()
    

def convertParquetToPickleDF (path_to_parquet, col_to_select, PklDF):
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html
    df = pd.read_parquet(path_to_parquet,columns=col_to_select)
    df.to_pickle(PklDF)
    del df


def convertCrossrefJSONDumpToAvro(path_to_crossref, path_to_avro, physicalCores, virtualPerCore, start_year, end_year):
    client = Client(n_workers=physicalCores, threads_per_worker=virtualPerCore)
    print(client)
    schema = {'name': 'Crossref', 'doc': "Crossref data to metadata",
          'type': 'record',
          'fields': [
              {'name': 'DOI', 'type': ["string", "null"]},
              {'name': 'Title', 'type': ["string", "null"]},
              {'name': 'Venue', 'type': ["string", "null"]},
              {'name': 'Type', 'type': ["string", "null"]},
              {'name': 'Authors', 'type': ["string", "null"]},
              {'name': 'Subject', 'type': ["string", "null"]},
              {'name': 'ISSN', 'type': ["string", "null"]},
              {'name': 'ISBN', 'type': ["string", "null"]},
              {'name': 'Year', 'type': ["string", "null"]}
          ]
         }

    b = db.read_text(path_to_crossref)\
    .filter(lambda x: len(x)>2)\
    .map(lambda x: x.replace('{"items":[','').replace(',\n',''))\
    .map(json.loads)\
    .map(flatten)\
    .filter(lambda record: (int(record['Year']) >= start_year) & (int(record['Year']) <= end_year))\
    .to_avro(path_to_avro, schema)
    
    client.close()