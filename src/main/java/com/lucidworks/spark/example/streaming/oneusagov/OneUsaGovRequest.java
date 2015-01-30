package com.lucidworks.spark.example.streaming.oneusagov;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

/**
 * Maps 1.usa.gov JSON fields to a Java object
 * see: http://www.usa.gov/About/developer-resources/1usagov.shtml
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OneUsaGovRequest implements Serializable {

  public static OneUsaGovRequest parse(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(json, OneUsaGovRequest.class);
  }

  @JsonProperty("a")
  public String userAgent;

  @JsonProperty("c")
  public String countryCode;

  @JsonProperty("nk")
  public int knownUser;

  @JsonProperty("g")
  public String globalBitlyHash;

  @JsonProperty("h")
  public String encodingUserBitlyHash;

  @JsonProperty("l")
  public String encodingUserLogin;

  @JsonProperty("hh")
  public String shortUrlCName;

  @JsonProperty("r")
  public String referringUrl;

  @JsonProperty("u")
  public String longUrl;

  @JsonProperty("t")
  public long timestamp;

  @JsonProperty("hc")
  public long hashCreatedOn;

  @JsonProperty("gr")
  public String geoRegion;

  @JsonProperty("tz")
  public String timezone;

  @JsonProperty("cy")
  public String geoCityName;

  @JsonProperty("al")
  public String acceptLanguage;

  @JsonProperty("ll")
  public double[] latlon;

  public int hashCode() {
    return (globalBitlyHash != null ? globalBitlyHash.hashCode() : 31) +
           (userAgent != null ? userAgent.hashCode() : 31) +
           (timezone != null ? timezone.hashCode() : 31) +
           (countryCode != null ? countryCode.hashCode() : 31);
  }

  public boolean equals(Object other) {
    if (other == null) return false;
    if (other == this) return true;
    if (!(other instanceof OneUsaGovRequest)) return false;
    OneUsaGovRequest that = (OneUsaGovRequest)other;
    return this.timestamp == that.timestamp && this.knownUser == that.knownUser &&
      this.hashCreatedOn == that.hashCreatedOn &&
      eq(this.userAgent, that.userAgent) && eq(this.countryCode, that.countryCode) && 
      eq(this.globalBitlyHash, that.globalBitlyHash) && eq(this.encodingUserBitlyHash, that.encodingUserBitlyHash) && 
      eq(this.encodingUserLogin, that.encodingUserLogin) && eq(this.shortUrlCName, that.shortUrlCName) &&
      eq(this.referringUrl, that.referringUrl) && eq(this.longUrl, that.longUrl) &&
      eq(this.timezone, that.timezone) && eq(this.geoCityName, that.geoCityName) &&
      eq(this.acceptLanguage, that.acceptLanguage) && eq(this.latlon, that.latlon);
  }

  private final boolean eq(String lhs, String rhs) {
    return (lhs != null) ? lhs.equals(rhs) : (rhs == null);
  }
  
  private final boolean eq(double[] lhs, double[] rhs) {
    if (lhs != null) {
      return (rhs != null && lhs.length == rhs.length) ? Arrays.deepEquals(new Object[]{lhs}, new Object[]{rhs}) : false;
    }
    return (rhs == null); // consider both being null equiv
  }

  @Override
  public String toString() {
    ObjectMapper jsonObjectMapper = new ObjectMapper();
    try {
      return jsonObjectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
