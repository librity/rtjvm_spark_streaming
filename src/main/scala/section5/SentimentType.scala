package section5

sealed trait SentimentType {}

/**
  * - NLP Library: Scores are between 0 and 5
  */
object SentimentType {
  def fromScore(score: Double): SentimentType = {
    if (score < 0) return NOT_UNDERSTOOD
    if (score < 1) return VERY_NEGATIVE
    if (score < 2) return NEGATIVE
    if (score < 3) return NEUTRAL
    if (score < 4) return POSITIVE
    if (score < 5) return VERY_POSITIVE
    NOT_UNDERSTOOD
  }
}

case object VERY_NEGATIVE extends SentimentType

case object NEGATIVE extends SentimentType

case object NEUTRAL extends SentimentType

case object POSITIVE extends SentimentType

case object VERY_POSITIVE extends SentimentType

case object NOT_UNDERSTOOD extends SentimentType
