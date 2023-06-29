# million song dataset artist genre analysis
 treat data from spotify to get conclusions about realtions between artist and their music gender and the impact in playcount by users

 data obtained from kaggle: https://www.kaggle.com/datasets/undefinenull/million-song-dataset-spotify-lastfm?select=Music+Info.csv

 first, it uses 'user-listening-history' to get the playcount of each song in the dataset, then crosses that information with the song-info to append that data.

 Then we tried to group songs by artist-genre, but the data had many songs with null in the genre column and didn't gave the information we wanted, but then, noticing that the tag column had more information about the genres of the song, we used that field in the end, by splitting the tags into multiple tuples (the script also contains the previous genre column approach).

 The result is for every artist, how many songs for what genre of music, and how many playcount has for that specific genre, ordered by the number of songs in that genre. To in the end be able to compare less explored genres for an artist, and how well it went compared with their most prefered genres.