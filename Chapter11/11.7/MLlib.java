

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

public final class MLlib {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("JavaBookExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // Load 2 types of emails from text files: spam and ham (non-spam).
    // Each line has text from one email.
    JavaRDD<String> spam = sc.textFile("../11.3/spam.txt");
    JavaRDD<String> ham = sc.textFile("../11.3/ham.txt");

    // Create a HashingTF instance to map email text to vectors of 100 features.
    final HashingTF tf = new HashingTF(100);

    // Each email is split into words, and each word is mapped to one feature.
    // Create LabeledPoint datasets for positive (spam) and negative (ham) examples.
    JavaRDD<LabeledPoint> positiveExamples = spam.map(new Function<String, LabeledPoint>() {
      @Override public LabeledPoint call(String email) {
        return new LabeledPoint(1, tf.transform(Arrays.asList(email.split(" "))));
      }
    });
    JavaRDD<LabeledPoint> negativeExamples = ham.map(new Function<String, LabeledPoint>() {
      @Override public LabeledPoint call(String email) {
        return new LabeledPoint(0, tf.transform(Arrays.asList(email.split(" "))));
      }
    });
    JavaRDD<LabeledPoint> trainingData = positiveExamples.union(negativeExamples);
    trainingData.cache(); // Cache data since Logistic Regression is an iterative algorithm.

    // Create a Logistic Regression learner which uses the LBFGS optimizer.
    LogisticRegressionWithSGD lrLearner = new LogisticRegressionWithSGD();
    // Run the actual learning algorithm on the training data.
    LogisticRegressionModel model = lrLearner.run(trainingData.rdd());

    // Test on a positive example (spam) and a negative one (ham).
    // First apply the same HashingTF feature transformation used on the training data.
    Vector posTestExample =
        tf.transform(Arrays.asList("O M G GET cheap stuff by sending money to ...".split(" ")));
    Vector negTestExample =
        tf.transform(Arrays.asList("Hi Dad, I started studying Spark the other ...".split(" ")));
    // Now use the learned model to predict spam/ham for new emails.
    System.out.println("--------------------------------------------------------------------------------------------");
    System.out.println("Prediction for positive test example: " + model.predict(posTestExample));
    System.out.println("Prediction for negative test example: " + model.predict(negTestExample));
    System.out.println("--------------------------------------------------------------------------------------------");

    sc.stop();
  }
}
