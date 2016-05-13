rm -r -f bin/output bin/intermediate_output
cd bin
jar cf WordRelatedness.jar Job1*.class Job2*.class LocalRunner*.class WordPair*.class LimitedTreeSet*.class Node*.class stop_words.txt
hadoop jar WordRelatedness.jar LocalRunner ../input output 15
cd ..