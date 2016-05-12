import java.util.TreeSet;

public class LimitedTreeSet extends TreeSet<Node<?>> {

	private static final long serialVersionUID = 1L;
	private int _k;
	
	public LimitedTreeSet(int k) {
		super();
		_k = k;
	}
	
	@Override
	public boolean add(Node<?> e) {
		boolean res = super.add(e);
		
		if (super.size() > _k)
			super.pollFirst();
		
		return res;
	}
	
	// quick test
	public static void main(String[] args) {
		LimitedTreeSet t = new LimitedTreeSet(10);
		
		for (int i = 20; i > 0; i--) {
			t.add(new Node<String>(0.1*i, "SOME PAYLOAD"));
		}
		
		System.out.println(t);
	}
	
}


class Node<T> implements Comparable<Node<T>> {
    double statistic;
    T value;

    Node(double statistic, T value) {
        this.statistic = statistic;
        this.value = value;
    }

    @Override
    public int compareTo(Node<T> other) {
        if (this.statistic >= other.statistic) {
            return 1;
        } else {
            return -1;
        }
    }
    
    public String toString() {
		return statistic + ":" + value.toString();
    }
}



