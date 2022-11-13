# Section 8 - Notes

## Twitter For Developers

- https://developer.twitter.com/en
- https://github.com/twitterdev
- https://medium.com/javarevisited/how-to-access-twitter-v2-api-with-java-6d707003e43e
- https://developer.twitter.com/en/docs/authentication/guides/v2-authentication-mapping
- https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/introduction
- https://www.toptal.com/apache/apache-spark-streaming-twitter

## Twitter API V2

- https://github.com/twitterdev/Twitter-API-v2-sample-code/tree/main/Filtered-Stream
- https://github.com/twitterdev/Twitter-API-v2-sample-code/tree/main/Sampled-Stream
- https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/introduction
- https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/introduction

Sample Stream Tweets:

```json
[
  (...),
  {
    "data": {
      "author_id": "1589746231496032256",
      "created_at": "2022-11-12T02:44:03.000Z",
      "edit_history_tweet_ids": [
        "1591260506752708608"
      ],
      "id": "1591260506752708608",
      "text": "RT @OrhanKarl5: aynı #süleymanpasa ispatlanmıştır koyunları kendine sevseydik kadar #tekirdag  Sabırlı sandalye bana gunlerdir daha dedigin…"
    },
    "includes": {
      "users": [
        {
          "created_at": "2022-11-07T22:27:17.000Z",
          "id": "1589746231496032256",
          "name": "Hüseyin Çoban",
          "username": "Hseyino80716715"
        },
        {
          "created_at": "2022-07-15T21:02:52.000Z",
          "id": "1548050447612534784",
          "name": "Orhan Karlı",
          "username": "OrhanKarl5"
        }
      ]
    }
  },
  (...)
]
```

Filtered Stream Tweets:

```json
[
  (...),
  {
    TODO
  },
  (...)
]
```

## Twitter API v2 Java SDKE

- https://dev.to/twitterdev/a-guide-to-working-with-the-twitter-api-v2-in-java-using-twitter-api-java-sdk-c8n
- https://github.com/twitterdev/twitter-api-java-sdk/

## Scala

### Parse JSON

- https://stackoverflow.com/questions/30884841/converting-json-string-to-a-json-object-in-scala
- https://stackoverflow.com/questions/33307386/how-to-create-a-json-object-in-scala
- https://github.com/spray/spray-json
- https://www.playframework.com/documentation/2.8.x/ScalaJson
- https://index.scala-lang.org/playframework/play-json
- https://github.com/scala/scala-parser-combinators
