package section5

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap

import java.util.Properties
import scala.collection.JavaConverters._

object SentimentAnalysis {
  /**
    * NLP Librari configuration
    */
  def creatNlpProps(): Properties = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")

    props
  }

  /**
    * Extract sentiment score by sentence and average it out
    */
  def detectSentiment(message: String): SentimentType = {
    val pipeline = new StanfordCoreNLP(creatNlpProps())
    val annotation = pipeline.process(message)


    /**
      * Split text into sentences and attach a score to each
      */
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
    val sentiments = sentences
      .map { sentence: CoreMap =>
        val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])


        /**
          * Convert each score to Double
          */
        RNNCoreAnnotations.getPredictedClass(tree).toDouble
      }


    val averageSentiment =
      if (sentiments.isEmpty) -1
      else sentiments.sum / sentiments.length

    SentimentType.fromScore(averageSentiment)
  }
}
