package org.hobbit.odin.odinevaluationmodule;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import javax.xml.bind.DatatypeConverter;

import org.apache.jena.rdf.model.RDFNode;

public class ResultValue {
    Object value;

    public Object getValue() {
        return this.value;
    }

    @Override
    public String toString(){
        if(value == null)
            return "Null";
        
        else{
            return this.value.toString();
        }
        
    }
    
    public ResultValue(RDFNode v) {
        if (!v.isLiteral()) {
            this.value = (String) v.asResource().getURI();
        } else {

            try {
                this.value = (Double) DatatypeConverter.parseDouble(v.asLiteral().getLexicalForm());
            } catch (Exception e) {
                try {
                    Calendar cal = DatatypeConverter.parseDateTime(v.asLiteral().getLexicalForm());
                    long temp = cal.getTimeInMillis();
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                    df.setTimeZone(TimeZone.getTimeZone("GMT"));
                    this.value = (String) df.format(temp);
                } catch (Exception e1) {
                    this.value = (String) DatatypeConverter.parseString(v.asLiteral().getLexicalForm());
                }
            }

        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ResultValue other = (ResultValue) obj;
        if (value == null) {
            return other.value != null;
        } else {
            if (other.value instanceof Double && value instanceof Double) {
                if (Math.abs((Double) other.value - (Double) value) < (0.0001d * (Double) Math.abs((Double) value))) {
                    return true;
                } else
                    return false;
            } else if (other.value instanceof String && value instanceof String) {
                return ((String) value).equals((String) other.value);
            }
        }
        return false;
    }

}
