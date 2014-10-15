package edu.mki.bachelor.dataimport;

public class Triple<E, V, X> {
	private final E one;
	private final V two;
	private final X three;

	Triple(E one, V two, X three) {
		this.one = one;
		this.two = two;
		this.three = three;
	}

	public E getOne() {
		return one;
	}

	public V getTwo() {
		return two;
	}

	public X getThree() {
		return three;
	}

	public boolean equals(Triple<E, V, X> triple) {
		if (this.one == triple.one && this.two == triple.two
				&& this.three == triple.three) {
			return true;
		} else
			return false;
	}

	public String toString() {
		return this.one + ";" + this.two + ";" + this.three;
	}

	public String toString(String seperator) {
		return this.one + seperator + this.two + seperator + this.three;
	}
}
