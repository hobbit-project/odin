package org.hobbit.odin.util;

/**
 * This class defines constants of the Odin benchmark.
 * 
 * @author Kleanthi Georgala (georgala@informatik.uni-leipzig.de)
 * @version 1.0
 *
 */
public final class OdinConstants {
    // =============== STRUCTURED BENCHMARK CONTROLLER CONSTANTS ===============
    public static final byte MIN_MAX_FROM_DATAGENERATOR = (byte) 200;
    public static final byte OVERALL_MIN_MAX = (byte) 201;
    
    public static final byte BULK_LOAD_FROM_DATAGENERATOR = (byte) 250;
    public static final byte BULK_LOAD_FROM_CONTROLLER = (byte) 251;


    // =============== STRUCTURED DATA GENERATOR CONSTANTS ===============
    public static final String GENERATOR_SEED = "generator_seed";
    public static final String GENERATOR_POPULATION = "generator_population";
    public static final String GENERATOR_DATASET = "generator_dataset";
    public static final String GENERATOR_MIMICKING_OUTPUT = "generator_mimicking-output";
    public static final String GENERATOR_INSERT_QUERIES_COUNT = "generator_insert-queries-count";
    public static final String GENERATOR_BENCHMARK_DURATION = "generator_benchmark_duration";

    // =============== STRUCTURED EVALUATION MODULE CONSTANTS ===============

    public static final String EVALUATION_AVERAGE_TASK_DELAY = "evaluation_task-delay";

    public static final String EVALUATION_MICRO_AVERAGE_RECALL = "evaluation_micro-average-recall";
    public static final String EVALUATION_MICRO_AVERAGE_PRECISION = "evaluation_micro-average-precision";
    public static final String EVALUATION_MICRO_AVERAGE_FMEASURE = "evaluation_micro-average-fmeasure";

    public static final String EVALUATION_MACRO_AVERAGE_RECALL = "evaluation_macro-average-recall";
    public static final String EVALUATION_MACRO_AVERAGE_PRECISION = "evaluation_macro-average-precision";
    public static final String EVALUATION_MACRO_AVERAGE_FMEASURE = "evaluation_macro-average-fmeasure";
    
    public static final String EVALUATION_MAX_TPS = "evaluation_max-tps";
    public static final String EVALUATION_AVERAGE_TPS = "evaluation_average-tps";
    
    public static final String EVALUATION_TASKS_EVALUATION_RECALL = "evaluation_task-evaluation-recall";
    public static final String EVALUATION_TASKS_EVALUATION_TPS = "evaluation_task-evaluation-tps";
    public static final String EVALUATION_TASKS_EVALUATION_DELAY = "evaluation_task-evaluation-delay";
    public static final String EVALUATION_TASKS_EVALUATION_PRECISION = "evaluation_task-evaluation-precision";
    public static final String EVALUATION_TASKS_EVALUATION_FMEASURE = "evaluation_task-evaluation-fmeasure";

    
}
