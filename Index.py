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

import csv
import pandas as pd
import snap
import seaborn as sns
import matplotlib.pyplot as plt

def generateVenueRank(journalHash, journalGraph, outputJournalFile, alpha):
    #Ref: http://snap.stanford.edu/snappy/doc/reference/streams.html
    hashes = snap.TFIn(journalHash) #Loading hash file
    mapping = snap.TStrIntSH (hashes) #Creating mapping list containing NodeIDs and Names
    
    graphFile = snap.TFIn(journalGraph) #Loading graph file.
    graph = snap.TNEANet.Load(graphFile) #Loading graph file into variable
    #Ref: http://snap.stanford.edu/snappy/doc/reference/GetPageRank.html 
    PageRankH = snap.TIntFltH() #Creating Integer float hash for PR
    snap.GetPageRank(graph, PageRankH, alpha, 1e-8, graph.GetNodes()) #Saving PageRank of the graph in var created above.
    
    InDegV = graph.GetNodeInDegV() #Using Snap's InDegree function to get InDegrees
    OutDegV = graph.GetNodeOutDegV() #Using Snap's OutDegree function to get OutDegrees
    with open(outputJournalFile, 'w', newline='', encoding="utf-8") as file: #Creating file AuthorCitationInfo.csv
        journalcsv = csv.writer(file) #Using CSV writer
        journalcsv.writerow(['Journal Name', 'Rank', 'Citation']) #Writing the column names
        for (item1,item2,item3) in zip(PageRankH,InDegV,OutDegV): #Traversing all the items in PageRank Hash
            pr = PageRankH[item1] #Separating PageRank using subscript. here item is NodeID
            name = mapping.GetKey(item1).replace("_"," ") #Using mapping hash separating the Name of author
            journalcsv.writerow([name,pr,item2.GetVal2()]) #Writing NodeID, Author Name, PR Score
    
    print("Venue Ranking Completed") #Checking when PR is done


def generateAuthorRank(authorHash, authorGraph, outputAuthorFile, alpha):
    authorHashes = snap.TFIn(authorHash) #Loading hash file
    authorMapping = snap.TStrIntSH (authorHashes) #Creating mapping list containing NodeIDs and Names
    graphFile = snap.TFIn(authorGraph) #Loading graph file.
    authorGraph = snap.TNEANet.Load(graphFile) #Loading graph file into variable
    
    #Ref: http://snap.stanford.edu/snappy/doc/reference/GetPageRank.html 
    PageRankH = snap.TIntFltH() #Creating Integer float hash for PR
    snap.GetPageRank(authorGraph, PageRankH, alpha, 1e-8, authorGraph.GetNodes()) #Saving PageRank of the graph in var created above.
    
    #Ref: http://snap.stanford.edu/snappy/doc/reference/GetNodeOutDegV.html
    InDegV = authorGraph.GetNodeInDegV() #Using Snap's InDegree function to get InDegrees
    OutDegV = authorGraph.GetNodeOutDegV() #Using Snap's OutDegree function to get OutDegrees
    with open(outputAuthorFile, 'w', newline='', encoding="utf-8") as file: #Creating file AuthorCitationInfo.csv
        authorcsv = csv.writer(file) #Using CSV writer
        authorcsv.writerow(['Author Name', 'Rank', 'Citation']) #Writing the column names
        for (item1,item2,item3) in zip(PageRankH,InDegV,OutDegV): #Traversing all the items in PageRank Hash
            pr = PageRankH[item1] #Separating PageRank using subscript. here item is NodeID
            name = authorMapping.GetKey(item1).replace("_"," ") #Using mapping hash separating the Name of author
            authorcsv.writerow([name,pr,item2.GetVal2()]) #Writing NodeID, Author Name, PR Score
    
    print("Author Ranking Completed") #Checking when PR is done

def generatePublicationRank(journalFile, metaData, citNet, articlegraph, articleHash, authorFile, outputFile, beta, gamma, printflag):
    df = pd.read_pickle(metaData)
    COCI = pd.read_pickle(citNet)

    journalDF = pd.read_csv(journalFile)
    authorDF = pd.read_csv(authorFile)

    graphFile = snap.TFIn(articlegraph)
    graph = snap.TNEANet.Load(graphFile)
    hashes = snap.TFIn(articleHash)
    mapping = snap.TStrIntSH (hashes)
    
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rank.html
    journalDF['Percentile_rank']=journalDF.Rank.rank(method='dense',pct=True)
    authorDF['Percentile_rank']=authorDF.Rank.rank(method='dense',pct=True)

    cut_labels = [1,2,3,4,5,6]
    cut_bins = [0,0.5,0.75,0.90,0.95,0.99,1.0]
    
    #Ref: https://pandas.pydata.org/docs/reference/api/pandas.cut.html
    journalDF['PRank'] = pd.cut(journalDF['Percentile_rank'], bins=cut_bins, labels=cut_labels).astype(int)
    authorDF['PRank'] = pd.cut(authorDF['Percentile_rank'], bins=cut_bins, labels=cut_labels).astype(int)
    
    df['Year']=df.Year.astype(int)
    df['DOI']=df.index
    df = pd.merge(df, authorDF, left_on='Author', right_on='Author Name')
    df = pd.merge(df, journalDF, left_on='Venue', right_on='Journal Name')
    df = df.groupby('DOI').mean()
    df["Rank"] = (df.PRank_x*beta) + (df.PRank_y*(1-beta))
    
    #Ref: https://snap.stanford.edu/snappy/doc/reference/GetNodeInDegV.html
    InDegV = graph.GetNodeInDegV() 
    OutDegV = graph.GetNodeOutDegV() 
    artDict=[]
    for (cit,ref) in zip(InDegV,OutDegV):
        artDict.append({'DOI':mapping.GetKey(cit.GetVal1()), 'Citation':cit.GetVal2(), 'Reference':ref.GetVal2()})

    artDF = pd.DataFrame(artDict)
    artDF = pd.merge(artDF, df, left_on='DOI', right_on='DOI')
    
    COCI = COCI.merge(artDF, left_on='citing', right_on='DOI')
    COCI = COCI.merge(artDF, left_on='cited', right_on='DOI')
    RankDF = COCI.groupby('cited').agg({'Rank_x': 'sum', 'Rank_y': 'max','Citation_y': 'max', 'Year_y': 'max', 'PRank_x_y': 'max', 'PRank_y_y': 'max'})
    
    RankDF['Percentile_rank']=RankDF.groupby('Year_y')['Rank_x'].rank(method='dense',pct=True)
    RankDF['CitScore'] = pd.cut(RankDF['Percentile_rank'], bins=cut_bins, labels=cut_labels).astype(int)
    
    RankDF['Percentile_rank']=RankDF.groupby('Year_y')['Rank_y'].rank(method='dense',pct=True)
    RankDF['PubScore'] = pd.cut(RankDF['Percentile_rank'], bins=cut_bins, labels=cut_labels).astype(int)
    
    RankDF['Percentile_rank']=RankDF.groupby('Year_y')['Citation_y'].rank(method='dense',pct=True)
    RankDF['Citation'] = pd.cut(RankDF['Percentile_rank'], bins=cut_bins, labels=cut_labels).astype(int)
    
    RankDF['Score'] = (RankDF.CitScore * gamma) + (RankDF.PubScore * (1-gamma))

    RankDF = RankDF.rename(columns={"PRank_x_y": "AutScore","PRank_y_y": "VenScore"})

    RankDF.index.names = ['DOI']
    RankDF.to_csv(outputFile,index=True,columns=['AutScore','VenScore','PubScore','CitScore','Score','Citation'], encoding='utf-8')
        
    if printflag: print("Publication Ranking Completed")
    
def generateQualitativeResults(PublicationInfoCSV, PublicationRankCSV, metaData, corrF, gridF):
    
    RankDF = pd.read_csv(PublicationInfoCSV).set_index('DOI')
    
    correlation_mat = RankDF.rank().corr()
    sns.heatmap(correlation_mat, annot = True)
    #plt.show()
    plt.savefig(corrF, format='svg', dpi=1200)
    
    DF = pd.read_pickle(metaData)
    DF = DF.drop("Author",axis=1)
    RankDF = DF.groupby('DOI').first().merge(RankDF, left_index=True, right_index=True)
    RankDF.index.names = ['DOI']
     
    RankDF.sort_values(['Year','Score'], ascending=[True, False]).groupby('Year').head(5).to_csv(PublicationRankCSV, index=True)
    
    RankDF = pd.read_csv(PublicationRankCSV)
    RankDF.index = RankDF.index + 1
    RankDF['Venue'] = RankDF['Venue'].str.replace('&', '\&')
    RankDF['Title'] = RankDF['Title'].str.replace('&', '\&')
    return RankDF
    
def generateComparison(metaData, PublicationInfoCSV, DOItestPkl, printflag):
    DF = pd.read_pickle(metaData)

    RankDF = pd.read_csv(PublicationInfoCSV).set_index('DOI')
    refDF = pd.read_pickle(DOItestPkl)
    
    a = set(list(RankDF.index))
    b = set(refDF)
    c = set(RankDF.sort_values('Score', ascending=False).head(1000).index.to_list())
    d = set(DF.index.to_list())

    if printflag: print (len(a), len(b), len(c), len(c.intersection(b)),len(d.intersection(b)), len(c.intersection(b)) / len(d.intersection(b)))
    return len(c.intersection(b))
    
def generateQuantitativeResults (VenueInfoCSV, metaData, citNet, PublicationGraph, PublicationHash, AuthorInfoCSV, PublicationInfoCSV, DOItestPkl):

    df = pd.DataFrame(columns = ['beta' , 'gamma', 'val'])
    k=0
    for beta in range(0, 11, 1):
            for gamma in range(0, 11, 1):
                generatePublicationRank(VenueInfoCSV, metaData, citNet, PublicationGraph, PublicationHash, AuthorInfoCSV, PublicationInfoCSV, beta/10, gamma/10, False)
                df.loc[k] = [beta/10, gamma/10, generateComparison(metaData, PublicationInfoCSV, DOItestPkl, False)]
                k=k+1

    print ("min\n", df.sort_values(by='val').min())
    print ("max\n", df.sort_values(by='val').max())
    print ("mean\n", df.sort_values(by='val').mean())

def generateBaseline (citNetFull, DOIPkl, DOItestPkl):
    COCI = pd.read_pickle(citNetFull)
    DOIs = pd.read_pickle(DOIPkl)
    refDF = pd.read_pickle(DOItestPkl)

    CitedLst = COCI[COCI.citing.isin(DOIs)].cited.tolist()
    CoCitationDF = COCI[COCI.cited.isin(CitedLst)]
    CoCitedLst = CoCitationDF.citing.value_counts().head(1000)
    a = CoCitedLst.index
    b = set(refDF)
    print ("refs in Bibliographic Coupling Top", len(a), len(a.intersection(b)))

    CitedLst = COCI[COCI.cited.isin(DOIs)].citing.tolist()
    CoCitationDF = COCI[COCI.citing.isin(CitedLst)]
    CoCitedLst = CoCitationDF.cited.value_counts().head(1000)
    a = CoCitedLst.index
    b = set(refDF)
    print ("refs in Cocited list Top", len(a), len(a.intersection(b)))