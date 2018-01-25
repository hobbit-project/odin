package org.hobbit.odin.odinevaluationmodule;

import org.apache.jena.rdf.model.RDFNode;

public class ResultValue {
    Object value;

    public ResultValue(RDFNode v) {
        if (!v.isLiteral()) {
            this.value = (String) v.asResource().getURI();
        } else {
            try {
                this.value = (Double) v.asLiteral().getDouble();
            } catch (Exception e) {
                this.value = (String) v.asLiteral().getLexicalForm();
            }
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null){
            return false;
        }
        if (getClass() != obj.getClass()){
            return false;
        }
        ResultValue other = (ResultValue) obj;
        if (value == null) {
            return other.value != null;
        } else {
            if (other.value instanceof Double && value instanceof Double) {
                if (Math.abs((Double) other.value - (Double) value) < (0.0001d * (Double) value)) {
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
