package org.apache.phoenix.query;

import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarchar;
import org.junit.Test;

/**
 * Created by elomore on 11/11/16.
 */
public class simpleTest {

    @Test
    public void test(){
        System.out.println(PVarchar.INSTANCE.isCoercibleTo(PTimestamp.INSTANCE));

    }
}
