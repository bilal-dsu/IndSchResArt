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

import pandas as pd
import snap
import numpy as np

def saveGraph (Dimension, CitNet, NodeList):
    #Ref: http://snap.stanford.edu/snappy/doc/tutorial/tutorial.html#saving-and-loading-graphs
    filename = Dimension+".graph" #Creating graph file
    FOut = snap.TFOut(filename) #Using snap's built in function to save
    CitNet.Save(FOut) #Saving file
    FOut.Flush() #Flushing the file value

    name = Dimension+".hash" #Creating Hash file
    FOut = snap.TFOut(name) #Using snap's built in function to save
    NodeList.Save(FOut) #Saving file
    FOut.Flush() #Flushing the file value
    
    print("Graph created for %s " % Dimension) #To make sure graph is created
    print("Number of Nodes: %d, Number of Edges: %d" % (CitNet.GetNodes(), CitNet.GetEdges())) #Checking the node count if graph is generated successfully or not.
    
    #Ref: http://snap.stanford.edu/snappy/doc/reference/info.html
    #snap.PrintInfo(CitNet, Dimension+" Citation Network", Dimension+".stats", False)


def generatePublicationCitationNetwork(Dimension, metaData, citNet, random, K): 
    COCI = pd.read_pickle(citNet)
   
    artNodeList = snap.TStrIntSH()
    artCitNet = snap.TNEANet.New()
    
    artList = list(COCI.citing.unique())  
    artList.extend(list(COCI.cited.unique()))
    artList = list(set(artList))
    
    for art in artList:
        artCitNet.AddNode(artNodeList.AddKey(art))
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.itertuples.html
    for citation in COCI.itertuples(index=False):
        citingNode=artNodeList.GetKeyId(citation.citing)
        citedNode=artNodeList.GetKeyId(citation.cited)
        artCitNet.AddEdge(citingNode,citedNode)
    artCitNet.DelSelfEdges()
    print("Number of Nodes: %d, Number of Edges: %d" % (artCitNet.GetNodes(), artCitNet.GetEdges())) #Checking the node count if graph is generated successfully or not.

    #Ref: http://snap.stanford.edu/snappy/doc/reference/kcore.html
    KCore = artCitNet.GetKCore(K)
    if KCore.Empty():
        print('No Core exists for K=%d' % K)
    else:
        
        for i in range(3):
            NodeV = KCore.GetNodeOutDegV() #Get Node Vector
            nodeList = []
            NIdV = []

            for n in NodeV:
                if n.GetVal2()>=K:
                    NodeID = n.GetVal1() #Getting ID 
                    DOI = artNodeList.GetKey(NodeID)
                    nodeList.append(DOI)
                    NIdV.append(NodeID)
                
            KCore = KCore.GetSubGraph(NIdV)
            COCI = COCI[(COCI.citing.isin(nodeList)) & (COCI.cited.isin(nodeList))]           
        
        if (random): 
            KCore = snap.TNEANet.New()

            df = pd.read_pickle(metaData)
            COCI = COCI.merge(df.drop(['Authors', 'Venue'], axis=1), left_on='citing', right_index=True)
            COCI.citing = COCI.groupby("Year").citing.sample(frac=1).values
            COCI = COCI.drop(['Year','Venue_citing'], axis=1)
            COCI = COCI.merge(df.drop(['Authors', 'Year'], axis=1), left_on='citing', right_index=True)
            COCI = COCI.rename(columns={"Venue": "Venue_citing"})
            for art in artList:
                KCore.AddNode(artNodeList.AddKey(art))
            #Ref: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.itertuples.html
            for citation in COCI.itertuples(index=False):
                citingNode=artNodeList.GetKeyId(citation.citing)
                citedNode=artNodeList.GetKeyId(citation.cited)
                KCore.AddEdge(citingNode,citedNode)
            print("Random network created")
            del df  
        KCore = KCore.GetSubGraph(NIdV)
        COCI.to_pickle(citNet)
        del COCI
        saveGraph (Dimension, KCore, artNodeList)
    
def generateVenueCitationNetwork(Dimension, citNet, K):
    COCI = pd.read_pickle(citNet)
    
    jnlNodeList = snap.TStrIntSH()
    jnlCitNet = snap.TNEANet.New()
    
    jnlList = list(COCI.Venue_citing.unique())  
    jnlList.extend(list(COCI.Venue_cited.unique()))
    jnlList = list(set(jnlList))
    
    for jnl in jnlList:
        jnlCitNet.AddNode(jnlNodeList.AddKey(jnl))
    
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.itertuples.html
    for citation in COCI.itertuples(index=False):
        citingNode=jnlNodeList.GetKeyId(citation.Venue_citing)
        citedNode=jnlNodeList.GetKeyId(citation.Venue_cited)
        jnlCitNet.AddEdge(citingNode,citedNode)
    print("Number of Nodes: %d, Number of Edges: %d" % (jnlCitNet.GetNodes(), jnlCitNet.GetEdges())) #Checking the node count if graph is generated successfully or not.

    #Ref: http://snap.stanford.edu/snappy/doc/reference/kcore.html
    KCore = jnlCitNet.GetKCore(K)
    if KCore.Empty():
        print('No Core exists for K=%d' % K)
    else:
        while (True):
            NodeV = KCore.GetNodeOutDegV() #Get Node Vector
            nodeList = []
            NIdV = []

            flag = False
            for n in NodeV:
                if n.GetVal2()>=K:
                    NodeID = n.GetVal1() #Getting ID 
                    DOI = jnlNodeList.GetKey(NodeID)
                    nodeList.append(DOI)
                    NIdV.append(NodeID)
                else:
                    flag=True
            if (flag):
                KCore = KCore.GetSubGraph(NIdV)
                COCI = COCI[(COCI.Venue_citing.isin(nodeList)) & (COCI.Venue_cited.isin(nodeList))]
            else:
                break
  
        COCI.to_pickle(citNet)
        del COCI
        saveGraph (Dimension, KCore, jnlNodeList)

def generateAuthorID (MetaDataFile, artCitNet, autCitNet, autCitNetLst):
    df = pd.read_pickle(MetaDataFile)
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.Series.str.split.html
    df["Authors"] = df["Authors"].str.split(",")
    df = df.explode("Authors")
    df[['orcid','name']]=df["Authors"].str.split(" ",n=1,expand=True)
    
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.DataFrameGroupBy.nunique.html
    cntName = df.groupby('name').orcid.nunique()
    autOrcidList = cntName[cntName>1].index.tolist()
    
    #Ref: https://note.nkmk.me/en/python-numpy-where/
    condition = (df.name.isin(autOrcidList)) & (df.orcid!='')
    df['Author'] = np.where(condition,df.orcid,df.name)
    
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.core.groupby.GroupBy.nth.html
    df = df.groupby('DOI').nth([0,1,2,3,4,5,6,7,-2,-1])    
    df = df.drop(['orcid','name','Authors'],axis=1)
    df.to_pickle(MetaDataFile)
    
    df = df.drop(['Venue', 'Year'], axis=1)
    autDF = df.groupby('Author').first().reset_index()
    autDF['autID']=autDF.index
    autDF.to_pickle(autCitNetLst)
    
    df['DOI']=df.index
    df = df.merge(autDF, on='Author')
    df = df.drop(['Author'], axis=1)
    df = df.set_index('DOI')

    COCI = pd.read_pickle(artCitNet)
    #COCI = COCI.drop(['Venue_citing', 'Venue_cited'], axis=1)
    COCI = COCI.merge(df, left_on='citing', right_index=True)
    COCI = COCI.merge(df, left_on='cited', right_index=True, suffixes=('_citing', '_cited'))
    #COCI = COCI.drop(['citing', 'cited'], axis=1)
    del df  

    COCI.to_pickle(autCitNet)
    del COCI
    

def generateAuthorCitationNetwork(Dimension, autCitNetF, autCitNetLst, K):
    COCI = pd.read_pickle(autCitNetF)
    
    autCitNet = snap.TNEANet.New()
    
    autList = list(COCI.autID_citing.unique())  
    autList.extend(list(COCI.autID_cited.unique()))
    autList = list(set(autList))

    for a in autList:
        autCitNet.AddNode(int(a))
    
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.itertuples.html
    for citation in COCI.itertuples(index=False):
       autCitNet.AddEdge(citation.autID_citing,citation.autID_cited)
    print("Number of Nodes: %d, Number of Edges: %d" % (autCitNet.GetNodes(), autCitNet.GetEdges())) #Checking the node count if graph is generated successfully or not.

    autDF = pd.read_pickle(autCitNetLst)
    autNodeList = snap.TStrIntSH()
    for aut in autDF.itertuples(index=False):
        autNodeList.AddKey(aut.Author)
    
    #Ref: http://snap.stanford.edu/snappy/doc/reference/kcore.htm    
    KCore = autCitNet.GetKCore(K)
    if KCore.Empty():
        print('No Core exists for K=%d' % K)
    else:
        while (True):
            NodeV = KCore.GetNodeOutDegV() #Get Node Vector
            nodeList = []
            NIdV = []

            flag = False
            for n in NodeV:
                if n.GetVal2()>=K:
                    NodeID = n.GetVal1() #Getting ID 
                    DOI = autNodeList.GetKey(NodeID)
                    nodeList.append(DOI)
                    NIdV.append(NodeID)
                else:
                    flag=True
            if (flag):
                KCore = KCore.GetSubGraph(NIdV)
                COCI = COCI[(COCI.autID_citing.isin(nodeList)) & (COCI.autID_cited.isin(nodeList))]
            else:
                break
        
        COCI.to_pickle(autCitNetF)
        del COCI
        saveGraph (Dimension, KCore, autNodeList)

