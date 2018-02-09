package org.hobbit.odin.local;

import static org.junit.Assert.*;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.hobbit.odin.odinevaluationmodule.ResultValue;
import org.junit.Test;

public class ResultValueTest {

    @Test
    public void testDDPlain() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        double d = 0.1111;
        Literal dNumber = newModel.createLiteral(String.valueOf(d));
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber);

        double d2 = 0.11110000000009;
        Literal dNumber2 = newModel.createLiteral(String.valueOf(d2));
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber2);

        ResultValue v = new ResultValue(dNumber);
        ResultValue v2 = new ResultValue(dNumber2);

        assertTrue(v.getValue() instanceof Double);
        assertTrue(v2.getValue() instanceof Double);
        assertTrue(v.equals(v2));

    }

    @Test
    public void testDFPlain() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        double d = 0.1111;
        Literal dNumber = newModel.createLiteral(String.valueOf(d));
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber);

        float d2 = 0.11110000000009f;
        Literal dNumber2 = newModel.createLiteral(String.valueOf(d2));
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber2);

        ResultValue v = new ResultValue(dNumber);
        ResultValue v2 = new ResultValue(dNumber2);

        assertTrue(v.getValue() instanceof Double);
        assertTrue(v2.getValue() instanceof Double);
        assertTrue(v.equals(v2));

    }

    ///////////////////////////////////////////////////////////////////////////
    @Test
    public void testDDTyped() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        double d = 0.1111;
        Literal dNumber = newModel.createTypedLiteral(d, XSDDatatype.XSDdouble);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber);

        double d2 = 0.11110000000009;
        Literal dNumber2 = newModel.createTypedLiteral(d2, XSDDatatype.XSDdouble);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber2);

        ResultValue v = new ResultValue(dNumber);
        ResultValue v2 = new ResultValue(dNumber2);

        assertTrue(v.getValue() instanceof Double);
        assertTrue(v2.getValue() instanceof Double);
        assertTrue(v.equals(v2));

    }

    @Test
    public void testFFTyped() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        float d = 0.1111f;
        Literal dNumber = newModel.createTypedLiteral(d, XSDDatatype.XSDfloat);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber);

        float d2 = 0.11110000000009f;
        Literal dNumber2 = newModel.createTypedLiteral(d2, XSDDatatype.XSDfloat);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber2);

        ResultValue v = new ResultValue(dNumber);
        ResultValue v2 = new ResultValue(dNumber2);

        assertTrue(v.getValue() instanceof Double);
        assertTrue(v2.getValue() instanceof Double);
        assertTrue(v.equals(v2));

    }

    @Test
    public void testDFTyped() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        double d = 0.1111;
        Literal dNumber = newModel.createTypedLiteral(d, XSDDatatype.XSDdouble);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber);

        float d2 = 0.11110000000009f;
        Literal dNumber2 = newModel.createTypedLiteral(d2, XSDDatatype.XSDfloat);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber2);

        ResultValue v = new ResultValue(dNumber);
        ResultValue v2 = new ResultValue(dNumber2);

        assertTrue(v.getValue() instanceof Double);
        assertTrue(v2.getValue() instanceof Double);
        assertTrue(v.equals(v2));

    }

    @Test
    public void testStringValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        String str = "ble";
        Literal literal = newModel.createTypedLiteral(str, XSDDatatype.XSDstring);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        String str2 = " bl e";
        Literal literal2 = newModel.createTypedLiteral(str2, XSDDatatype.XSDnormalizedString);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);
        System.out.println(v2.getValue());
        assertTrue(!v.equals(v2));

        String str3 = " bl e";
        Literal literal3 = newModel.createLiteral(str3);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal3);
        ResultValue v3 = new ResultValue(literal3);
        assertTrue(v3.getValue() instanceof Literal);
        System.out.println(v3.getValue());

        assertTrue(!v.equals(v3));
        assertTrue(!v2.equals(v3));

    }

    @Test
    // possible issue here
    public void testDatesValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("2001-10-26", XSDDatatype.XSDdate);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal5 = newModel.createTypedLiteral("2001-10-26+02:00", XSDDatatype.XSDdate);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal5);
        ResultValue v5 = new ResultValue(literal5);
        assertTrue(v5.getValue() instanceof Literal);

        Literal literal4 = newModel.createTypedLiteral("2001-10-26+02:00", XSDDatatype.XSDdate);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal4);
        ResultValue v4 = new ResultValue(literal4);
        assertTrue(v4.getValue() instanceof Literal);

        assertTrue(!v4.equals(v));
        assertTrue(v4.equals(v5));

        Literal literal2 = newModel.createLiteral("2001-10-26+02:00");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);

        Literal literal3 = newModel.createLiteral("2001-10-26");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal3);
        ResultValue v3 = new ResultValue(literal3);
        assertTrue(v3.getValue() instanceof Literal);

        assertTrue(!v2.equals(v3));
        assertTrue(!v.equals(v3));
    }

    @Test
    // possible issue here
    public void testTimesValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("09:30:10Z", XSDDatatype.XSDtime);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal2 = newModel.createTypedLiteral("09:30:10+02:00", XSDDatatype.XSDtime);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);

        assertTrue(!v.equals(v2));
    }

    @Test
    // possible issue here
    public void testDateTimeValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("2002-05-30T09:30:10-06:00", XSDDatatype.XSDdateTime);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal2 = newModel.createLiteral("2002-05-30T09:30:10Z");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);

        assertTrue(!v.equals(v2));
    }

    @Test
    // possible issue here
    public void testYearValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("2001+02:00", XSDDatatype.XSDgYear);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal2 = newModel.createLiteral("2001");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        // NOT STRING
        assertTrue(v2.getValue() instanceof Double);

        assertTrue(!v.equals(v2));
    }

    @Test
    // possible issue here
    public void testMonthValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("--11Z", XSDDatatype.XSDgMonth);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal2 = newModel.createLiteral("--11+02:00");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);

        assertTrue(!v.equals(v2));
    }

    @Test
    // possible issue here
    public void testDurationValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("PT2M10S", XSDDatatype.XSDduration);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal2 = newModel.createLiteral("PT1004199059S");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);

        assertTrue(!v.equals(v2));
    }

    @Test
    public void testhexBinaryValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("0FB8", XSDDatatype.XSDhexBinary);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);
        System.out.println(v.getValue());

        Literal literal2 = newModel.createTypedLiteral("0fb8", XSDDatatype.XSDhexBinary);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);
        System.out.println(v2.getValue());

        assertTrue(!v.equals(v2));

        Literal literal3 = newModel.createLiteral("0FB8");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal3);
        ResultValue v3 = new ResultValue(literal3);
        assertTrue(v3.getValue() instanceof Literal);
        System.out.println(v3.getValue());

        assertTrue(!v.equals(v3));

    }

    @Test
    public void testBooleanValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("false", XSDDatatype.XSDboolean);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Literal);

        Literal literal2 = newModel.createLiteral("false");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Literal);

        assertTrue(!v.equals(v2));

    }

    @Test
    public void testDecimalValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("-.456", XSDDatatype.XSDdecimal);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Double);

        Literal literal2 = newModel.createLiteral("-0.456");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Double);

        assertTrue(v2.equals(v));

    }

    @Test
    public void testIntegerValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("1231", XSDDatatype.XSDinteger);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Double);

        Literal literal2 = newModel.createLiteral("1231.01");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Double);

        assertTrue(v2.equals(v));

    }

    @Test
    public void testByteValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("-34", XSDDatatype.XSDbyte);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Double);

        Literal literal2 = newModel.createLiteral("-34.000000000000009");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Double);

        assertTrue(v2.equals(v));

    }

    @Test
    public void testShortValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("-0000000000000000000005", XSDDatatype.XSDshort);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Double);

        Literal literal2 = newModel.createLiteral("-5");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Double);

        assertTrue(v2.equals(v));
    }

    @Test
    public void testLongValues() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        Literal literal = newModel.createTypedLiteral("-9223372036854775808", XSDDatatype.XSDlong);
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal);
        ResultValue v = new ResultValue(literal);
        assertTrue(v.getValue() instanceof Double);

        Literal literal2 = newModel.createLiteral("-9223372036854775808.23232");
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), literal2);
        ResultValue v2 = new ResultValue(literal2);
        assertTrue(v2.getValue() instanceof Double);

        assertTrue(v2.equals(v));

    }

    @Test
    public void testFloat() {
        Model newModel = ModelFactory.createDefaultModel();
        Resource experiment = newModel.createResource("http://w3id.org/bench/123");

        float d = 0.1111f;
        Literal dNumber = newModel.createLiteral(String.valueOf(d));
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber);

        float d2 = 0.11110000000009f;
        Literal dNumber2 = newModel.createLiteral(String.valueOf(d2));
        newModel.add(experiment, newModel.createProperty("http://w3id.org/bench/value"), dNumber2);

        ResultValue v = new ResultValue(dNumber);
        ResultValue v2 = new ResultValue(dNumber2);

        assertTrue(v.getValue() instanceof Double);
        assertTrue(v2.getValue() instanceof Double);
        assertTrue(v.equals(v2));

    }
}
