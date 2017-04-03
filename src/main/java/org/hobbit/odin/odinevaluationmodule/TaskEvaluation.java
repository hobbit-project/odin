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

    private HashMap<String, Double> kpis = new HashMap<String, Double>();

    public TaskEvaluation(double rec, double pr, double tps, double answerDelay) {
        kpis.put("Recall", rec);
        kpis.put("Precision", pr);
        kpis.put("TPS", tps);
        kpis.put("Fmeasure", (double) (2.0 * rec * pr) / (double) (rec + pr));
        kpis.put("Delay", answerDelay);

    }

    public HashMap<String, Double> getKPIs() {
        return kpis;
    }

}
