REGISTER datafu-1.2.0.jar;
REGISTER piggybank-0.17.0.jar;
define Enumerate datafu.pig.bags.Enumerate('1');

-- Load Data
raw_userlistening = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/grupo5/user-listening-history.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (track_id:chararray, user_id:chararray, playcount:int);

raw_musicinfo = LOAD 'hdfs://cm:9000/uhadoop2023/proyects/grupo5/music-info.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (track_id:chararray,name,artist:chararray,spotify_preview_url,spotify_id,tags,genre:chararray,year,duration_ms,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature);


-- Count playcount per track_id
userlistening_sum = FOREACH (GROUP raw_userlistening BY track_id) {
    unique_users = DISTINCT user_id;
    GENERATE
        group AS track_id,
        SUM(raw_userlistening.playcount) AS playcount:int;
};

joined_data = JOIN raw_musicinfo BY track_id LEFT OUTER, userlistening_sum by track_id;

-- Get track_id playcount
songs_with_playcount = FOREACH joined_data GENERATE raw_musicinfo::track_id, raw_musicinfo::artist, raw_musicinfo::genre, (userlistening_sum::playcount IS NOT NULL ? userlistening_sum::playcount : 0) AS playcount:int;

genrecount = FOREACH (GROUP songs_with_playcount BY (artist, genre)) GENERATE FLATTEN(group), COUNT($1) AS count:int, SUM($1.playcount) AS playcount:int;

-- Count genre playcount and rank
groupagain = GROUP genrecount BY $0;
ranked_artists_genres = FOREACH groupagain {
	ordergenre = ORDER $1 BY count DESC;
	GENERATE FLATTEN(Enumerate(ordergenre));
};


-- Tokenize tags, count playcount and rank
songs_with_playcount_with_tags = FOREACH joined_data GENERATE raw_musicinfo::track_id, raw_musicinfo::artist, raw_musicinfo::tags, (userlistening_sum::playcount IS NOT NULL ? userlistening_sum::playcount : 0) AS playcount:int;

token_tags = FOREACH songs_with_playcount_with_tags GENERATE raw_musicinfo::track_id, raw_musicinfo::artist, TOKENIZE(raw_musicinfo::tags, ', ') AS genre, playcount;

splitted_tags = FOREACH token_tags GENERATE $0 AS track_id, $1 AS artist, FLATTEN($2) AS genre, $3 AS playcount;

genrecount_with_tags = FOREACH (GROUP splitted_tags BY (artist, genre)) GENERATE FLATTEN(group), COUNT($1) AS count:int, SUM($1.playcount) AS playcount:int;

groupagain_with_tags = GROUP genrecount_with_tags BY $0;
ranked_artists_tags = FOREACH groupagain_with_tags {
    ordergenre = ORDER $1 BY count DESC;
    GENERATE FLATTEN(Enumerate(ordergenre));
};


-- Store output files
STORE ranked_artists_genres INTO '/uhadoop2023/proyects/grupo5/ranked_artists_genres';
STORE ranked_artists_tags INTO '/uhadoop2023/proyects/grupo5/ranked_artists_tags';
