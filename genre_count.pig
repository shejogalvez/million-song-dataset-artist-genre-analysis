REGISTER datafu-1.2.0.jar;
REGISTER piggybank-0.17.0.jar
define Enumerate datafu.pig.bags.Enumerate('1');

raw_userlistening = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/grupo5/user-listening-history.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (track_id:chararray, user_id:chararray, playcount:int);
userlistening_sum = FOREACH (GROUP raw_userlistening BY track_id) {
    unique_users = DISTINCT user_id;
    GENERATE
        group AS track_id,
        SUM(raw_userlistening.playcount) AS playcount:int;
}


raw_musicinfo = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/grupo5/music-info.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (track_id:chararray,name,artist:chararray,spotify_preview_url,spotify_id,tags,genre:chararray,year,duration_ms,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature);


joined_data = JOIN raw_musicinfo BY track_id LEFT OUTER, userlistening_sum by track_id;

songs_with_playcount = FOREACH joined_data GENERATE raw_musicinfo::track_id, raw_musicinfo::artist, raw_musicinfo::tags, (userlistening_sum::playcount IS NOT NULL ? userlistening_sum::playcount : 0) AS playcount:int;

B = FOREACH songs_with_playcount GENERATE raw_musicinfo::track_id, raw_musicinfo::artist, TOKENIZE(raw_musicinfo::tags, ', ') AS genre, playcount;

splitted_tags = FOREACH B GENERATE $0 AS track_id, $1 AS artist, FLATTEN($2) AS genre, $3 AS playcount;

genrecount = FOREACH (GROUP splitted_tags BY (artist, genre)) GENERATE FLATTEN(group), COUNT($1) AS count:int, SUM($1.playcount) AS playcount:int;

groupagain = GROUP genrecount BY $0;
ranked_artistgenre = FOREACH groupagain {
	ordergenre = ORDER $1 BY count DESC;
	GENERATE FLATTEN(Enumerate(ordergenre));
};

STORE top_genres_result INTO '/uhadoop2023/proyects/grupo5/ranked_artistgenre';

