import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class WordPair implements Writable,WritableComparable<WordPair> {
	        private Text word;
	        private Text neighbor;
	        private BooleanWritable isSum;
	        
	        public WordPair(Text word, Text neighbor, BooleanWritable isSum) {
	            this.word = word;
	            this.neighbor = neighbor;
	            this.isSum = isSum;
	        }

	        public WordPair(String word, String neighbor, boolean isSum) {
	            this.word.set(word);
	            this.neighbor.set(neighbor);
	            this.isSum.set(isSum);
	        }

	        public WordPair() {
	            this.word = new Text();
	            this.neighbor = new Text();
	            this.isSum = new BooleanWritable(false);
	        }

	        @Override
	        public int compareTo(WordPair other) {
	            int returnVal = this.word.compareTo(other.getWord());
	            if(returnVal != 0) {
	                return returnVal;
	            }
	            if(isSum.get())
	            	if(other.isSum.get())
	            		return 0;
	            	else
	            		return -1;
	            else if(other.isSum.get())
	                return 1;
	            
	            return this.neighbor.compareTo(other.getNeighbor());
	        }



	        public WordPair read(DataInput in) throws IOException {
	            WordPair wordPair = new WordPair();
	            wordPair.readFields(in);
	            return wordPair;
	        }

	        @Override
	        public void write(DataOutput out) throws IOException {
	            word.write(out);
	            neighbor.write(out);
	            isSum.write(out);
	        }

	        @Override
	        public void readFields(DataInput in) throws IOException {
	            word.readFields(in);
	            neighbor.readFields(in);
	            isSum.readFields(in);
	        }

	        @Override
	        public String toString() {
	        	if (isSum.get())
		            return "{word=["+word+"]}";
	        	else
		            return "{word=["+word+"]"+
		                   " neighbor=["+neighbor+"]}";
	        }

	        @Override
	        public boolean equals(Object o) {
	            if (this == o) return true;
	            if (o == null || getClass() != o.getClass()) return false;

	            WordPair wordPair = (WordPair) o;

	            if (neighbor != null ? !(neighbor.equals(wordPair.neighbor)) : wordPair.neighbor != null) return false;
	            if (word != null ? !word.equals(wordPair.word) : wordPair.word != null) return false;

	            return (isSum.get() == true && wordPair.isSum.get() == true);
	        }

	        @Override
	        public int hashCode() {
	            int result = word != null ? word.hashCode() : 0;
	            result += isSum.hashCode();
	            result = 389 * result + (neighbor != null ? neighbor.hashCode() : 0);
	            return result;
	        }

	        public void setWord(String word){
	            this.word.set(word);
	        }
	        public void setNeighbor(String neighbor){
	            this.neighbor.set(neighbor);
	        }
	        
	        public void setIsSum(boolean isSum){
	            this.isSum.set(isSum);
	        }
	        
	        public Text getWord() {
	            return word;
	        }

	        public Text getNeighbor() {
	            return neighbor;
	        }
	        
	        public BooleanWritable getIsSum() {
	            return isSum;
	        }
}
