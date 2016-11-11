package org.apache.phoenix.query;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.ByteBasedRegexpReplaceFunction;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.RoundFunction;
import org.apache.phoenix.expression.function.ToDateFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PTimestamp;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by lomoree on 2016-10-06.
 */
@Ignore
public class tableTest extends BaseConnectionlessQueryTest{

    @Test
    public void test() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("CREATE TABLE t (k1 DATE PRIMARY KEY, k2 DECIMAL)");
        conn.prepareStatement("SELECT * FROM t where k1=TO_DATE('2016-06-06') AND k2=FLOOR(ROUND(CEIL(2016)))");
        //conn.createStatement().execute("CREATE SEQUENCE IF NOT EXISTS seq0 START WITH 1 INCREMENT BY 1");
        //ResultSet rs = conn.createStatement().executeQuery("select CURRENT VALUE FOR seq0");
        //rs.next();

        //sql = "SELECT * FROM t WHERE k1 = TO_DATE(TO_CHAR(CURRENT_DATE))";
        //sql = "SELECT * FROM t where k1=TO_DATE('2016-06-06')";
        //PreparedStatement pStmt = conn.prepareStatement(sql);
        conn.close();
    }

    @Test
    public void testx2() throws Exception {
        FunctionParseNode.BuiltInFunction d = ToDateFunction.class.getAnnotation(FunctionParseNode.BuiltInFunction.class);
        FunctionParseNode.BuiltInFunction d2 = FloorDecimalExpression.class.getAnnotation(FunctionParseNode.BuiltInFunction.class);
        System.out.println(PDate.INSTANCE.isCastableTo(PTimestamp.INSTANCE));

    }

    @Test
    public void testx3() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        PhoenixConnection pc = conn.unwrap(PhoenixConnection.class);
        ByteBasedRegexpReplaceFunction.class.getMethod("create", List.class, StatementContext.class).invoke(null, new ArrayList<Expression>(), new StatementContext((PhoenixStatement)pc.createStatement()));

    }
}
