package com.huldar.ch04.mapreduce;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.Iterator;

/**
 * TODO
 *
 * @author huldar
 * @date 2019/4/22 14:55
 */
public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {

    private static final String L_MARK = "L";
    private Text productId = new Text();
    private Text locationId = new Text("undefined");

    @Override
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {

        Iterator<PairOfStrings> iterator = values.iterator();
        if (iterator.hasNext()) {
            PairOfStrings firstPair = iterator.next();
            System.out.println("first = " + firstPair.toString());
            if (L_MARK.equals(firstPair.getLeftElement())) {
                locationId.set(firstPair.getRightElement());
            }
        }

        while (iterator.hasNext()) {
            PairOfStrings productPair = iterator.next();
            productId.set(productPair.getRightElement());
            context.write(productId, locationId);
        }


    }
}
