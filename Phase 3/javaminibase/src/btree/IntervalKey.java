package btree;

import global.IntervalType;

/**  IntervalKey: It extends the KeyClass.
 *   It defines the interval Key.
 */
public class IntervalKey extends KeyClass {

    private IntervalType key;

    public String toString() {
        return key.toString();
    }

    /** Class constructor
     *  @param     value   the value of the integer key to be set
     */
    public IntervalKey(IntervalType value) {
        key = new IntervalType(value.start, value.end, value.level);
    }

    /** get a copy of the integer key
     *  @return the reference of the copy
     */
    public IntervalType getKey() {
        return new IntervalType(key.start, key.end, key.level);
    }

    /** set the integer key value
     */
    public void setKey(IntervalType value) {
        key = new IntervalType(value.start, value.end, value.level);
    }
}