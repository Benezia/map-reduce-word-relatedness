import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class WordPair implements Writable,WritableComparable<WordPair> {
	        private Text w1;
	        private Text w2;
	        private IntWritable decade;
	        private BooleanWritable isSum;
	        private BooleanWritable isTotalSum;
	        private IntWritable c1;
	        private IntWritable c2;
	        private IntWritable n;
	        
	        
	        public WordPair() {
	            this.w1 = new Text();
	            this.w2 = new Text();
	            this.isSum = new BooleanWritable(false);
	            this.isTotalSum = new BooleanWritable(false);
	            this.c1 = new IntWritable(0);
	            this.c2 = new IntWritable(0);
	            this.n = new IntWritable(0);
	            this.decade = new IntWritable(0);
	        }

	        @Override
	        public int compareTo(WordPair other) {
	        	int returnVal;
	        	if (isTotalSum.get()) {
	        		if (other.isTotalSum.get())
	        			return 0; 							// 		<*,*> == <*,*> 		(1/16) 	+1
	        		else
	        			return -1; 							// 		<*,*> < OTHER		(4/16) 	+3
	        	}
	        	
	        	if (other.isTotalSum.get()) 
	        		return 1; 								// 		OTHER > <*,*>		(7/16) 	+3
	        	
	        	if (isSum.get() && w2.toString().equals("**")) {
	        		if (other.isSum.get() && other.getW2().toString().equals("**"))
        				return w1.compareTo(other.getW1()); // 		<w2,*> ? <w2,*>		(8/16)	+1
        			else
        				return 1;							// 		<w2.*> > OTHER		(10/16) +2
	        	}

	        	
	        	if (isSum.get() && w2.toString().equals("*")) {
	        		if (other.isSum.get() && other.getW2().toString().equals("*")) 
	        			return w1.compareTo(other.getW1()); // 		<w1,*> ? <w1,*>		(11/16)	+1
	        		else {
	        			if (other.isSum.get())
	        				return -1;						// 		<w1,*> < <w2,*> 	(12/16)	+1
	        			else {
	        				returnVal = w1.compareTo(other.getW1());
	        				if (returnVal != 0)
	        					return returnVal; //w1 != other.w1	<w1,*> ? <w1,w2> 	(12.5/16) +0.5
	        				else
	        					return -1;		 // w1 == other.w1	<w1,*> < <w1,w2> 	(13/16)	+0.5
	        					
	        			}
	        		}
	        	}
	        	

	        	if (other.isSum.get()) {
	        		if (other.getW2().toString().equals("*")) {
        				returnVal = w1.compareTo(other.getW1());
        				if (returnVal != 0)
        					return returnVal; 	// w1 != other.w1	<w1,w2> ? <w1,*>	(13.5/16) +0.5
        				else	
        					return 1;			 // w1 == other.w1	<w1,w2> > <w1,*> 	(14/16)	+0.5
	        		}
	        			return -1;							// 		<w1,w2> < <w2,*>	(15/16) +1
	        	}        	
	        	
	            returnVal = this.w1.compareTo(other.getW1());
	            if(returnVal != 0) {
	                return returnVal;						
	            }
	            return this.w2.compareTo(other.getW2());	// 		<w1,w2> ? <w1,w2>	(16/16) +1
	        }



	        public WordPair read(DataInput in) throws IOException {
	            WordPair wordPair = new WordPair();
	            wordPair.readFields(in);
	            return wordPair;
	        }

	        @Override
	        public void write(DataOutput out) throws IOException {
	            w1.write(out);
	            w2.write(out);
	            decade.write(out);
	            isSum.write(out);
	            isTotalSum.write(out);
	            c1.write(out);
	            c2.write(out);
	            n.write(out);
	        }

	        @Override
	        public void readFields(DataInput in) throws IOException {
	            w1.readFields(in);
	            w2.readFields(in);
	            decade.readFields(in);
	            isSum.readFields(in);
	            isTotalSum.readFields(in);
	            c1.readFields(in);
	            c2.readFields(in);
	            n.readFields(in);          
	        }

	        @Override
	        public String toString() {
	        	if (isTotalSum.get())
	        		return "{}";
	        	if (isSum.get() && w2.toString().equals("*"))
		            return "w1=["+w1+"]";
	        	if (isSum.get() && w2.toString().equals("**"))
		            return "w2=["+w1+"]";
	        	
	            return "Decade=["+decade+"]" +
	            		" w1=["+w1+"]"+
	            		" w2=["+w2+"]"+
	            		" c1=["+c1+"]"+
	            		" c2=["+c2+"]"+
	            		" n=["+n+"]";
	        }

	        @Override
	        public boolean equals(Object o) {
	            if (this == o) return true;
	            if (o == null || getClass() != o.getClass()) return false;

	            WordPair wordPair = (WordPair) o;

	            if (w2 != null ? !(w2.equals(wordPair.w2)) : wordPair.w2 != null) return false;
	            if (w1 != null ? !w1.equals(wordPair.w1) : wordPair.w1 != null) return false;

	            return (isSum.get() == wordPair.isSum.get());
	        }

	        @Override
	        public int hashCode() {
	            int result = w1 != null ? w1.hashCode() : 0;
	            result += isSum.hashCode() * 191;
	            result += c1.hashCode() * 193;
	            result += c2.hashCode() * 197;
	            result += n.hashCode() * 199;
	            result += decade.hashCode() * 211;
	            result += isTotalSum.hashCode() * 223;
	            result = result + (w2 != null ? w2.hashCode() : 0) * 229;
	            return result;
	        }

	        public void setW1(String w1) { this.w1.set(w1); }
	        public void setW2(String w2) { this.w2.set(w2); }
	        public void setDecade (int decade) {this.decade.set (decade -(decade % 10)); }
	        public void setIsSum(boolean isSum) { this.isSum.set(isSum); }
	        public void setIsTotalSum(boolean isTotalSum) { this.isTotalSum.set(isTotalSum); }
	        public void setC1(int c1) { this.c1.set(c1); }
	        public void setC2(int c2) { this.c2.set(c2); }
	        public void setN(int n) { this.n.set(n); }
	        
	        public Text getW1() { return w1; }
	        public Text getW2() { return w2; }
	        public IntWritable getDecade() { return decade; }
	        public BooleanWritable getIsSum() { return isSum; }
	        public BooleanWritable getIsTotalSum() { return isTotalSum; }
	        public IntWritable getC1() { return c1; }
	        public IntWritable getC2() { return c2; }
	        public IntWritable getM() { return n; }
	        
}
