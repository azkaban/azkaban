drop table words;
create table words (freq int, word string) row format delimited fields terminated by '\t' stored as textfile;
describe words;
load data local inpath "res/input" into table words;
select * from words limit 10;
select freq, count(1) as f2 from words group by freq sort by f2 desc limit 10;


