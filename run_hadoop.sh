rm -r -f bin/output
cd bin
jar cf WordCount.jar WordCount*.class WordPair*.class stop_words.txt
hadoop jar WordCount.jar WordCount ../input output
cd ..