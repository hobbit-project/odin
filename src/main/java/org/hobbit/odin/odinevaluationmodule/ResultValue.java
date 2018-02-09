package org.hobbit.odin.odinevaluationmodule;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;

public class ResultValue {
    Object value;

    public Object getValue() {
        return this.value;
    }

    public ResultValue(RDFNode v) {

        if (!v.isLiteral()) {
            this.value = (String) v.asResource().getURI();
        } else {
            try {
                this.value = (Double) v.asLiteral().getDouble();
            } catch (Exception e) {
                //this.value = (String) v.asLiteral().getLexicalForm();
                this.value = (Literal) v.asLiteral();
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
            } else if (other.value instanceof Literal && value instanceof Literal) {
                return ((Literal) value).equals((Literal) other.value);
            }
        }
        return false;
    }

}
