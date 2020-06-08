package heap;

import global.Convert;
import global.IntervalType;


public class XMLRecord  {
	
	IntervalType interval;
	String tagName;
	
	int start;
	int end;
	int level;

	//length under control
	private int reclen;

	private byte[]  data;

	/** Default constructor
	 */
	public XMLRecord() {}

	/** another constructor
	 */
	public XMLRecord (int _reclen) {
		setRecLen (_reclen);
		data = new byte[_reclen];
	}

	/** constructor: convert a byte array to XMLRecord object.
	 * @param arecord a byte array which represents the XMLRecord object
	 */
	public XMLRecord(byte [] arecord) 
			throws java.io.IOException {
		setIntValue(arecord);
		setTagNameValue(arecord);
		data = arecord;
		setRecLen(tagName.length()); // TODO verify
	}

	/** constructor: translate a tuple to a XMLRecord object
	 *  it will make a copy of the data in the tuple
	 * @param atuple: the input tuple
	 */
	public XMLRecord(Tuple _atuple) 
			throws java.io.IOException{   
		data = new byte[_atuple.getLength()];
		
		data = _atuple.getTupleByteArray(); // TODO verify
		setRecLen(_atuple.getLength());

		setIntValue(data);
		setTagNameValue(data);
	}

	/** convert this class objcet to a byt	e array
	 *  this is used when you want to write this object to a byte array
	 */
	public byte [] toByteArray() 
			throws java.io.IOException {
		//    data = new byte[reclen];
		Convert.setIntValue(start, 0, data);
		Convert.setIntValue(end, 4, data);
		Convert.setIntValue(level, 8, data);
		Convert.setStrValue(tagName, 12, data);
		
		return data;
	}
	
	/** Sets integer fields for an XMLRecord -- start, end, level. */
	public void setIntValue(byte[] _data) throws java.io.IOException {
		start = Convert.getIntValue (0, _data);
		end = Convert.getIntValue(4, _data);
		level = Convert.getIntValue(8, _data);
	}
	
	/** Sets tagName field for an XMLRecord. */
	public void setTagNameValue(byte[] _data) throws java.io.IOException {
		tagName = Convert.getStrValue (12, _data, reclen-12);
	}

	//Other access methods to the size of the String field and 
	//the size of the record
	public void setRecLen (int size) {
		reclen = size;
	}

	public int getRecLength () {
		return reclen;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.start + " " + this.end + " " + this.level + " " + this.tagName + "\n\n";
	} 
}