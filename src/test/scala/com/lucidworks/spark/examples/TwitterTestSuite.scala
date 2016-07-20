package com.lucidworks.spark.examples

import com.lucidworks.spark.SparkSolrFunSuite
import com.lucidworks.spark.util.SolrSupport
import org.apache.solr.common.SolrInputDocument
import twitter4j.TwitterObjectFactory

class TwitterTestSuite extends SparkSolrFunSuite {

  test("Test twitter field object mapping") {
    val tweetJSON = "{ \"scopes\":{ \"place_ids\":[ \"place one\",\"place two\"]}, \"created_at\":\"Tue Mar 05 23:57:32 +0000 2013\", \"id\":309090333021581313, \"id_str\":\"309090333021581313\", \"text\":\"As announced, @anywhere has been retired per https:\\/\\/t.co\\/bWXjhurvwp The js file now logs a message to the console and exits quietly. ^ARK\", \"source\":\"web\", \"truncated\":false, \"in_reply_to_status_id\":null, \"in_reply_to_status_id_str\":null, \"in_reply_to_user_id\":null, \"in_reply_to_user_id_str\":null, \"in_reply_to_screen_name\":null, \"user\":{ \"id\":6253282, \"id_str\":\"6253282\", \"name\":\"Twitter API\", \"screen_name\":\"twitterapi\", \"location\":\"San Francisco, CA\", \"description\":\"The Real Twitter API. I tweet about API changes, service issues and happily answer questions about Twitter and our API. Don't get an answer? It's on my website.\", \"url\":\"http:\\/\\/dev.twitter.com\", \"entities\":{ \"url\":{ \"urls\":[ { \"url\":\"http:\\/\\/dev.twitter.com\", \"expanded_url\":null, \"indices\":[ 0, 22 ] } ] }, \"description\":{ \"urls\":[ ] } }, \"protected\":false, \"followers_count\":1533137, \"friends_count\":33, \"listed_count\":11369, \"created_at\":\"Wed May 23 06:01:13 +0000 2007\", \"favourites_count\":25, \"utc_offset\":-28800, \"time_zone\":\"Pacific Time (US & Canada)\", \"geo_enabled\":true, \"verified\":true, \"statuses_count\":3392, \"lang\":\"en\", \"contributors_enabled\":true, \"is_translator\":false, \"profile_background_color\":\"C0DEED\", \"profile_background_image_url\":\"http:\\/\\/a0.twimg.com\\/profile_background_images\\/656927849\\/miyt9dpjz77sc0w3d4vj.png\", \"profile_background_image_url_https\":\"https:\\/\\/si0.twimg.com\\/profile_background_images\\/656927849\\/miyt9dpjz77sc0w3d4vj.png\", \"profile_background_tile\":true, \"profile_image_url\":\"http:\\/\\/a0.twimg.com\\/profile_images\\/2284174872\\/7df3h38zabcvjylnyfe3_normal.png\", \"profile_image_url_https\":\"https:\\/\\/si0.twimg.com\\/profile_images\\/2284174872\\/7df3h38zabcvjylnyfe3_normal.png\", \"profile_banner_url\":\"https:\\/\\/si0.twimg.com\\/profile_banners\\/6253282\\/1347394302\", \"profile_link_color\":\"0084B4\", \"profile_sidebar_border_color\":\"C0DEED\", \"profile_sidebar_fill_color\":\"DDEEF6\", \"profile_text_color\":\"333333\", \"profile_use_background_image\":true, \"default_profile\":false, \"default_profile_image\":false, \"following\":null, \"follow_request_sent\":false, \"notifications\":null }, \"geo\":null, \"coordinates\":null, \"place\":null, \"contributors\":[ 7588892 ], \"retweet_count\":74, \"entities\":{ \"hashtags\":[ ], \"urls\":[ { \"url\":\"https:\\/\\/t.co\\/bWXjhurvwp\", \"expanded_url\":\"https:\\/\\/dev.twitter.com\\/blog\\/sunsetting-anywhere\", \"display_url\":\"dev.twitter.com\\/blog\\/sunsettin…\", \"indices\":[ 45, 68 ] } ], \"user_mentions\":[ { \"screen_name\":\"anywhere\", \"name\":\"Anywhere\", \"id\":9576402, \"id_str\":\"9576402\", \"indices\":[ 14, 23 ] } ] }, \"favorited\":false, \"retweeted\":false, \"possibly_sensitive\":false, \"lang\":\"en\" }"
    val tweetStatusObj = TwitterObjectFactory.createStatus(tweetJSON)
    // simple mapping from primitives to dynamic Solr fields using reflection
    val doc: SolrInputDocument = SolrSupport.autoMapToSolrInputDoc("tweet-" + tweetStatusObj.getId, tweetStatusObj, null)
    logger.info("Mapped to Document: " + doc.toString)

    assert(doc.containsKey("createdAt_tdt"))
    assert(doc.containsKey("lang_s"))
    assert(doc.containsKey("favoriteCount_i"))
    assert(doc.containsKey("source_s"))
    assert(doc.containsKey("retweeted_b"))
    assert(doc.containsKey("retweetCount_i"))
    assert(doc.containsKey("inReplyToStatusId_l"))
  }

}
