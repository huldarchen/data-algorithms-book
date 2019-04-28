package com.huldar.ch05;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 输出规约器的格式,以左边第一个值进行自然排序
 *
 * @author huldar
 * @date 2019/4/26 08:45
 */
public class PairOfWords implements WritableComparable<PairOfWords> {
    private String leftElement;
    private String rightElement;

    public PairOfWords() {
    }

    public PairOfWords(String left, String right) {
        set(left, right);
    }

    private void set(String left, String right) {
        leftElement = left;
        rightElement = right;
    }

    public void setLeftElement(String leftElement) {
        this.leftElement = leftElement;
    }

    public void setWord(String leftElement) {
        setLeftElement(leftElement);
    }

    public String getWord() {
        return leftElement;
    }

    public String getLeftElement() {
        return leftElement;
    }

    public String getRightElement() {
        return rightElement;
    }

    public void setRightElement(String rightElement) {
        this.rightElement = rightElement;
    }

    public void setNeighbor(String rightElement) {
        setRightElement(rightElement);
    }

    public String getNeighbor() {
        return rightElement;
    }

    public String getKey() {
        return leftElement;
    }

    public String getValue() {
        return rightElement;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PairOfWords)) {
            return false;
        }

        PairOfWords pair = (PairOfWords) obj;
        return leftElement.equals(pair.getLeftElement())
                && rightElement.equals(pair.getRightElement());
    }

    @Override
    public int hashCode() {
        return leftElement.hashCode() + rightElement.hashCode();
    }

    @Override
    public String toString() {
        return "(" + leftElement + ", " + rightElement + ")";
    }

    @Override
    public PairOfWords clone() {
        return new PairOfWords(this.leftElement, this.rightElement);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, leftElement);
        Text.writeString(dataOutput, rightElement);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        leftElement = Text.readString(dataInput);
        rightElement = Text.readString(dataInput);

    }

    @Override
    public int compareTo(PairOfWords other) {

        String left = other.getLeftElement();
        String right = other.getRightElement();

        if (leftElement.equals(left)) {
            return rightElement.compareTo(right);
        }

        return leftElement.compareTo(left);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            //register comparator
            super(PairOfWords.class);
        }

        /**
         * Compare two objects in binary.
         * b1[s1:l1] is the first object, and b2[s2:l2] is the second object.
         *
         * @param b1 The first byte array.
         * @param s1 The position index in b1. The object under comparison's starting index.
         * @param l1 The length of the object in b1.
         * @param b2 The second byte array.
         * @param s2 The position index in b2. The object under comparison's starting index.
         * @param l2 The length of the object under comparison in b2.
         * @return An integer result of the comparison.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            try {
                int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
                int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
                int firstStrL1 = readVInt(b1, s1);
                int firstStrL2 = readVInt(b2, s2);
                int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1, b2, s2 + firstVIntL2, firstStrL2);
                if (cmp != 0) {
                    return cmp;
                }

                int secondVIntL1 = WritableUtils.decodeVIntSize(b1[s1 + firstVIntL1 + firstStrL1]);
                int secondVIntL2 = WritableUtils.decodeVIntSize(b2[s2 + firstVIntL2 + firstStrL2]);
                int secondStrL1 = readVInt(b1, s1 + firstVIntL1 + firstStrL1);
                int secondStrL2 = readVInt(b2, s2 + firstVIntL2 + firstStrL2);
                return compareBytes(b1, s1 + firstVIntL1 + firstStrL1 + secondVIntL1, secondStrL1, b2,
                        s2 + firstVIntL2 + firstStrL2 + secondVIntL2, secondStrL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static {
        // register this comparator
        WritableComparator.define(PairOfWords.class, new Comparator());
    }

}
