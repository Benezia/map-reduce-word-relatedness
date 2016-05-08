import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class WordPair implements Writable,WritableComparable<WordPair> {
	        private Text word;
	        private Text neighbor;
	        private Boolean isSum;
	        
	        public WordPair(Text word, Text neighbor, Boolean... isSumArr) {
	            this.word = word;
	            this.neighbor = neighbor;
	            this.isSum = isSumArr.length > 0 ? isSumArr[0] : false;
	        }

	        public WordPair(String word, String neighbor, Boolean... isSumArr) {
	            this(new Text(word),new Text(neighbor));
	            this.isSum = isSumArr.length > 0 ? isSumArr[0] : false;
	        }

	        public WordPair() {
	            this.word = new Text();
	            this.neighbor = new Text();
	            this.isSum = false;
	        }

	        @Override
	        public int compareTo(WordPair other) {
	            int returnVal = this.word.compareTo(other.getWord());
	            if(returnVal != 0){
	                return returnVal;
	            }
	            if(this.neighbor.toString().equals("*") && isSum){
	                return -1;
	            }else if(other.getNeighbor().toString().equals("*") && isSum){
	                return 1;
	            }
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
	        }

	        @Override
	        public void readFields(DataInput in) throws IOException {
	            word.readFields(in);
	            neighbor.readFields(in);
	        }

	        @Override
	        public String toString() {
	        	if (isSum)
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

	            return (isSum == wordPair.isSum);
	        }

	        @Override
	        public int hashCode() {
	            int result = word != null ? word.hashCode() : 0;
	            result = 163 * result + (neighbor != null ? neighbor.hashCode() : 0);
	            return result;
	        }

	        public void setWord(String word){
	            this.word.set(word);
	        }
	        public void setNeighbor(String neighbor){
	            this.neighbor.set(neighbor);
	        }
	        
	        public void setIsSum(Boolean isSum){
	            this.isSum = isSum;
	        }
	        
	        public Text getWord() {
	            return word;
	        }

	        public Text getNeighbor() {
	            return neighbor;
	        }
	        
	        public Boolean getIsSum() {
	            return isSum;
	        }
}
