rm -r -f bin/output bin/intermediate_output
cd bin
jar cf JobHandler.jar Job1*.class Job2*.class JobHandler*.class WordPair*.class stop_words.txt
hadoop jar JobHandler.jar JobHandler ../input output 10
cd ..