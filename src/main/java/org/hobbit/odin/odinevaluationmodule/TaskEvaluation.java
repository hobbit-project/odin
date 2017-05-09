package org.hobbit.odin.odinevaluationmodule;

import java.util.HashMap;

/**
 * Task Evaluation class. It stores the KPIs of each task.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public class TaskEvaluation {

    private int receivedSize = 0;
    private int expectedSize = 0;
    
    public int getReceivedSize() {
        return receivedSize;
    }

    public int getExpectedSize() {
        return expectedSize;
    }
    private HashMap<String, Double> kpis = new HashMap<String, Double>();

    public TaskEvaluation(double rec, double pr, double tps, double answerDelay, int receivedSize, int expectedSize) {
        kpis.put("Recall", rec);
        kpis.put("Precision", pr);
        kpis.put("TPS", tps);
        if(rec == 0.0d && pr == 0.0d)
            kpis.put("Fmeasure", 0.0d);
        else
            kpis.put("Fmeasure", (double) (2.0 * rec * pr) / (double) (rec + pr));
        kpis.put("Delay", answerDelay);
        this.receivedSize = receivedSize;
        this.expectedSize = expectedSize;
    }

    public HashMap<String, Double> getKPIs() {
        return kpis;
    }

}
