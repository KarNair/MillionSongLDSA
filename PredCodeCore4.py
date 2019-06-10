
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, avg, when
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

import time
import sys
import os
import glob
import hdf5_getters
import re

# (8 cores, 16gb per machine) x 5 = 40 cores

# New API
spark_session = SparkSession        .builder        .master("spark://192.168.1.79:7077")         .appName("SongHotness")        .config("spark.dynamicAllocation.enabled", True)        .config("spark.shuffle.service.enabled", True)        .config("spark.dynamicAllocation.executorIdleTimeout","30s")        .config("spark.executor.cores",4)        .getOrCreate()

# Old API (RDD)
spark_context = spark_session.sparkContext
sqlContext = SQLContext(spark_context)

def rowUnpack(df):
        results={}
        for i in df:
            results.update(i.asDict())
        return(results)


# In[2]:


class Song:
    songCount = 0
    # songDictionary = {}

    def __init__(self, songID):
        self.id = songID
        Song.songCount += 1
        # Song.songDictionary[songID] = self

        self.albumName = None
        self.albumID = None
        self.artistID = None
        self.artistLatitude = None
        self.artistLocation = None
        self.artistLongitude = None
        self.artistName = None
        self.danceability = None
        self.duration = None
        self.energy = None
        self.genreList = []
        self.keySignature = None
        self.keySignatureConfidence = None
        self.lyrics = None
        self.popularity = None
        self.songhotttnesss = None
        self.tempo = None
        self.timeSignature = None
        self.timeSignatureConfidence = None
        self.title = None
        self.year = None

    def displaySongCount(self):
        print("Total Song Count %i" % Song.songCount)

    def displaySong(self):
        print("ID: %s" % self.id)  
    
    


# In[3]:



"""
Original:

Alexis Greenstreet (October 4, 2015) University of Wisconsin-Madison

This code is designed to convert the HDF5 files of the Million Song Dataset
to a CSV by extracting various song properties.

The script writes to a "SongCSV.csv" in the directory containing this script.

Please note that in the current form, this code only extracts the following
information from the HDF5 files:
AlbumID, AlbumName, ArtistID, ArtistLatitude, ArtistLocation,
ArtistLongitude, ArtistName, Danceability, Duration, KeySignature,
KeySignatureConfidence, SongID, Tempo, TimeSignature,
TimeSignatureConfidence, Title, and Year.

This file also requires the use of "hdf5_getters.py", written by
Thierry Bertin-Mahieux (2010) at Columbia University

Credit:
This HDF5 to CSV code makes use of the following example code provided
at the Million Song Dataset website 
(Home>Tutorial/Iterate Over All Songs, 
http://labrosa.ee.columbia.edu/millionsong/pages/iterate-over-all-songs),
Which gives users the following code to get all song titles:

#import os
#import glob
#import hdf5_getters
#def get_all_titles(basedir,ext='.h5') :
#    titles = []
#   for root, dirs, files in os.walk(basedir):
#        files = glob.glob(os.path.join(root,'*'+ext))
#        for f in files:
#            h5 = hdf5_getters.open_h5_file_read(f)
#            titles.append( hdf5_getters.get_title(h5) )
#            h5.close()
#   return titles 
"""

"""This code has been modified for python3 compatibility by Karthik Nair(28 May 2019) of Uppsala University (github.com/karnair)"""
"""This code has been adapted for conversion of .h5 files to PySpark Dataframe by Karthik Nair(3 June 2019) of Uppsala University (github.com/karnair)"""



# In[4]:


def h5toPysDf():
    
   
    song_number = []
    album_id = []
    artist_latitude = []
    artist_location = []
    artist_longitude = []
    artist_name =[]
    danceability = []
    duration = []
    energy = []
    key_signature = []
    key_signature_confidence = []
    song_id = []
    tempo = []
    song_hotttnesss = []
    time_signature = []
    time_signature_confidence = []
    title = []
    year = []
    
    
    
    csvRowString = ("SongID,AlbumID,ArtistLatitude,ArtistLocation,"+
            "ArtistLongitude,ArtistName,Danceability,Duration,Energy,KeySignature,"+
            "KeySignatureConfidence,SongHotttnesss,Tempo,TimeSignature,TimeSignatureConfidence,"+
            "Title,Year")
    
    csvAttributeList = re.split('\W+', csvRowString)
    
    for i, v in enumerate(csvAttributeList):
        csvAttributeList[i] = csvAttributeList[i].lower()
        csvRowString = ""
    
    basedir = "/home/ubuntu/MillionSongSubset/data" # "." As the default means the current directory
    ext = ".h5"
    
    for root, dirs, files in os.walk(basedir):        
        files = glob.glob(os.path.join(root,'*'+ext))
        for f in files:
            #print(f)

            songH5File = hdf5_getters.open_h5_file_read(f)
            song = Song(str(hdf5_getters.get_song_id(songH5File)))

            testDanceability = hdf5_getters.get_danceability(songH5File)
            # print type(testDanceability)
            # print ("Here is the danceability: ") + str(testDanceability)

            #song.artistID = str(hdf5_getters.get_artist_id(songH5File))
            song.albumID = str(hdf5_getters.get_release_7digitalid(songH5File))
            #song.albumName = str(hdf5_getters.get_release(songH5File))
            song.artistLatitude = str(hdf5_getters.get_artist_latitude(songH5File))
            song.artistLocation = str(hdf5_getters.get_artist_location(songH5File))
            song.artistLongitude = str(hdf5_getters.get_artist_longitude(songH5File))
            song.artistName = str(hdf5_getters.get_artist_name(songH5File))
            song.danceability = float(hdf5_getters.get_danceability(songH5File))
            song.duration = float(hdf5_getters.get_duration(songH5File))
            song.energy = float(hdf5_getters.get_energy(songH5File))
            # song.setGenreList()
            song.keySignature = float(hdf5_getters.get_key(songH5File))
            song.keySignatureConfidence = float(hdf5_getters.get_key_confidence(songH5File))
            # song.lyrics = None
            # song.popularity = None
            song.tempo = float(hdf5_getters.get_tempo(songH5File))
            song.songhotttnesss = float(hdf5_getters.get_song_hotttnesss(songH5File))
            song.timeSignature = float(hdf5_getters.get_time_signature(songH5File))
            song.timeSignatureConfidence = float(hdf5_getters.get_time_signature_confidence(songH5File))
            song.title = str(hdf5_getters.get_title(songH5File))
            song.year = str(hdf5_getters.get_year(songH5File))
            
            
            
            #csvRowString += str(song.songCount) + ","
            song_number.append(song.songCount)

            for attribute in csvAttributeList:
                # print "Here is the attribute: " + attribute + " \n"

                if attribute == 'AlbumID'.lower():
                    #csvRowString += song.albumID
                    album_id.append(song.albumID)
                elif attribute == 'ArtistLatitude'.lower():
                    latitude = song.artistLatitude
                    if latitude == 'nan':
                        latitude = ''
                    artist_latitude.append(latitude)
                elif attribute == 'ArtistLocation'.lower():
                    location = song.artistLocation
                    location = location.replace(',','')
                    artist_location.append(location) 
                elif attribute == 'ArtistLongitude'.lower():
                    longitude = song.artistLongitude
                    if longitude == 'nan':
                        longitude = ''
                    artist_longitude.append(longitude)   
                elif attribute == 'ArtistName'.lower():
                    artist_name.append(song.artistName)
                elif attribute == 'Danceability'.lower():
                    danceability.append(song.danceability)
                elif attribute == 'Duration'.lower():
                    duration.append(song.duration)
                elif attribute == 'Energy'.lower():
                    energy.append(song.energy)
                elif attribute == 'KeySignature'.lower():
                    key_signature.append(song.keySignature)
                elif attribute == 'KeySignatureConfidence'.lower():
                    # print "key sig conf: " + song.timeSignatureConfidence                                 
                    key_signature_confidence.append(song.keySignatureConfidence)
                elif attribute == 'SongID'.lower():
                    song_id.append(song.id)
                elif attribute == 'Tempo'.lower():
                    # print "Tempo: " + song.tempo
                    tempo.append(song.tempo)
                elif attribute == 'SongHotttnesss'.lower():
                    song_hotttnesss.append(song.songhotttnesss)
                elif attribute == 'TimeSignature'.lower():
                    time_signature.append(song.timeSignature)
                elif attribute == 'TimeSignatureConfidence'.lower():
                    # print "time sig conf: " + song.timeSignatureConfidence                                   
                    time_signature_confidence.append(song.timeSignatureConfidence)
                elif attribute == 'Title'.lower():
                    title.append(song.title)
                elif attribute == 'Year'.lower():
                    year.append(song.year)
                #"""else:
                 #   csvRowString += "Erm. This didn't work. Error. :( :(\n" """"

                #csvRowString += ","
            songH5File.close()
    pysp_df = sqlContext.createDataFrame(zip(song_number, album_id, artist_latitude, artist_location, artist_longitude, artist_name, danceability, duration, energy, key_signature, key_signature_confidence, song_id, tempo, song_hotttnesss, time_signature, time_signature_confidence, title, year), schema=['song_number', 'album_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name', 'danceability', 'duration', 'energy', 'key_signature', 'key_signature_confidence', 'song_id', 'tempo', 'song_hotttnesss', 'time_signature', 'time_signature_confidence', 'title', 'year'])
    return pysp_df


# In[5]:


#song_number, album_id, artist_latitude, artist_location, artist_longitude, artist_name, danceability, duration, energy, key_signature, key_signature_confidence, song_id, tempo, song_hotttnesss, time_signature, time_signature_confidence, title, year = main()
msd_df = h5toPysDf()


# In[6]:


#msd_df = sqlContext.createDataFrame(zip(song_number, album_id, artist_latitude, artist_location, artist_longitude, artist_name, danceability, duration, energy, key_signature, key_signature_confidence, song_id, tempo, song_hotttnesss, time_signature, time_signature_confidence, title, year), schema=['song_number', 'album_id', 'artist_latitude', 'artist_location', 'artist_longitude', 'artist_name', 'danceability', 'duration', 'energy', 'key_signature', 'key_signature_confidence', 'song_id', 'tempo', 'song_hotttnesss', 'time_signature', 'time_signature_confidence', 'title', 'year'])


# In[7]:


msd_df.show()


# In[8]:


msd_df.printSchema()


# In[9]:


start_time = time.time()


# In[10]:


msd_df = msd_df.na.fill(0)
msd_df1 = msd_df.filter(msd_df['song_hotttnesss'] > 0).agg(avg(col("song_hotttnesss"))).collect()


# In[11]:


average = rowUnpack(msd_df1)
average = average['avg(song_hotttnesss)']

msd_df_corrected = msd_df.withColumn("song_hotttnesss",               when(msd_df["song_hotttnesss"] == 0, average).otherwise(msd_df["song_hotttnesss"]))


# In[12]:


msd_df_corrected.show()


# In[13]:


msd_df_sub = msd_df_corrected.select("duration","key_signature","tempo","time_signature","song_hotttnesss")
#msd_df_sub.show()


# In[14]:


msd_sort = msd_df_sub.orderBy('duration', ascending=True)
#msd_sort.show()


# In[15]:


(training_data, test_data) = msd_sort.randomSplit([0.7, 0.3])
training_data =training_data.rdd.map(lambda x: LabeledPoint(x[4], x[:4]))
test_data =test_data.rdd.map(lambda x: LabeledPoint(x[4], x[:4]))

#training_data.take(5)
#test_data.take(5)


# In[16]:


model = DecisionTree.trainRegressor(training_data, categoricalFeaturesInfo={},
                                    impurity='variance', maxDepth=5, maxBins=32)


# In[17]:


model_tree = model.toDebugString()

predictions = model.predict(test_data.map(lambda x: x.features))
labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)

testMSE = labelsAndPredictions.map(lambda lp: (lp[0] - lp[1]) * (lp[0] - lp[1])).sum() / float(test_data.count())
end_time = time.time()
exec_time = end_time - start_time
print('Test Mean Squared Error = ' + str(testMSE))
print("--- %s seconds ---" % exec_time)


# In[18]:


input_cols = ["duration","key_signature","tempo","time_signature"]

for i, feat in enumerate(input_cols):
    model_tree = model_tree.replace('feature ' + str(i), feat)

print('Learned regression tree model: \n')
print(model_tree)


# In[19]:


spark_context.stop()


# In[20]:


#model.save(spark_context, "/home/ubuntu/MillionSongLDSA")
#sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeRegressionModel")

