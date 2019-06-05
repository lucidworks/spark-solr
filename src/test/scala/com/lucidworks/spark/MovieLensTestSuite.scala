package com.lucidworks.spark

import com.lucidworks.spark.util.{QueryConstants, ConfigurationConstants}
import org.apache.spark.sql.types.DoubleType

class MovieLensTestSuite extends MovielensBuilder {

  test("multiple nested where clauses with NOT and AND") {
    val sql =
      s"""
         | select genre from ${moviesColName} m where
         |     ((m.genre IN ('comedy') and (m.title != 'Here Comes Cookie (1935)')))
         |     OR
         |     (m.genre IN ('action') and m.title = 'Operation Dumbo Drop (1995)')
        """.stripMargin
    val results = sparkSession.sql(sql).collect()
    // (426-1) comedy results
    assert(results.count(r => r.getString(0) === "comedy") == 425)
    assert(results.count(r => r.getString(0) === "action") == 1)
  }

  test("multiple nested where clauses with NOT and multiple AND") {
    val sql =
      s"""
        | select genre from ${moviesColName} m where
        |     (m.genre IN ('comedy') and ((m.title != 'Here Comes Cookie (1935)') and (m.title != 'Coneheads (1993)')))
        |     OR
        |     (m.genre IN ('action') and m.title = 'Operation Dumbo Drop (1995)')
      """.stripMargin
    val results = sparkSession.sql(sql).collect()
    // (426-2) 424 comedy results
    assert(results.count(r => r.getString(0) === "comedy") == 424)
    assert(results.count(r => r.getString(0) === "action") == 1)
  }

  test("mutliple nested where clauses with NOT and multiple OR") {
    val sql =
      s"""
         | select genre from ${moviesColName} m where
         |     (m.genre IN ('comedy') and ((m.title != 'Here Comes Cookie (1935)') or (m.title != 'Coneheads (1993)')))
      """.stripMargin
    val results = sparkSession.sql(sql).collect()
    assert(results.length === 424)
  }

  test("Score column in SQL statement pushdown to Solr") {
    val sqlStmt = s"SELECT movie_id,title,score from ${moviesColName} where _query_='title_txt_en:dog' order by score desc LIMIT 100"
    val opts = Map(
      "zkhost" -> zkHost,
      "collection" -> moviesColName,
      ConfigurationConstants.REQUEST_HANDLER -> QueryConstants.QT_SQL,
      ConfigurationConstants.SOLR_SQL_STMT -> sqlStmt)
    val df = sparkSession.read.format("solr").options(opts).load()

    val schema = df.schema
    assert (schema.fieldNames.contains("score"))
    assert (schema("score").dataType == DoubleType)
    val rows = df.take(10)
    assert(rows(0).length==3)
  }

  test("Provide SQL schema via config") {
    val sqlStmt = s"SELECT movie_id,title,score from ${moviesColName} where _query_='title_txt_en:dog' order by score desc LIMIT 100"
    val sqlSchema = "movie_id:string,title:string,score:double"
    val opts = Map(
      "zkhost" -> zkHost,
      "collection" -> moviesColName,
      ConfigurationConstants.REQUEST_HANDLER -> QueryConstants.QT_SQL,
      ConfigurationConstants.SOLR_SQL_STMT -> sqlStmt,
      ConfigurationConstants.SOLR_SQL_SCHEMA -> sqlSchema)
    val df = sparkSession.read.format("solr").options(opts).load()

    val schema = df.schema
    assert (schema.fieldNames.contains("score"))
    assert (schema("score").dataType == DoubleType)
    val rows = df.take(10)
    assert(rows(0).length==3)
  }

  test("Test nested where clauses") {
    val opts = Map(
      "zkhost" -> zkHost,
      "collection" -> moviesColName,
      "query" -> "*:*",
      "filters" -> """genre:action,title:"Star Wars (1977)" OR title:"Power 98 (1995)" OR title:"Truth or Consequences, N.M. (1997)" OR title:"Romper Stomper (1992)" OR title:"Air Force One (1997)" OR title:"Alien 3 (1992)" OR title:"Best Men (1997)" OR title:"Hellraiser: Bloodline (1996)" OR title:"Alien: Resurrection (1997)" OR title:"Fair Game (1995)" OR title:"Star Trek: First Contact (1996)" OR title:"Long Kiss Goodnight, The (1996)" OR title:"Tomorrow Never Dies (1997)" OR title:"The Deadly Cure (1996)" OR title:"Jaws 2 (1978)" OR title:"Star Trek: The Wrath of Khan (1982)" OR title:"Metro (1997)" OR title:"Rumble in the Bronx (1995)" OR title:"Timecop (1994)" OR title:"Firestorm (1998)" OR title:"Star Trek VI: The Undiscovered Country (1991)" OR title:"Nick of Time (1995)" OR title:"Cliffhanger (1993)" OR title:"In the Line of Duty 2 (1987)" OR title:"Con Air (1997)" OR title:"Rock, The (1996)" OR title:"Crying Game, The (1992)" OR title:"Bloodsport 2 (1995)" OR title:"Mercury Rising (1998)" OR title:"Boot, Das (1981)" OR title:"Mighty Morphin Power Rangers: The Movie (1995)" OR title:"Specialist, The (1994)" OR title:"Bad Company (1995)" OR title:"Good Man in Africa, A (1994)" OR title:"Solo (1996)" OR title:"Palookaville (1996)" OR title:"Rising Sun (1993)" OR title:"Broken Arrow (1996)" OR title:"Heaven & Earth (1993)" OR title:"Star Trek: The Motion Picture (1979)" OR title:"Top Gun (1986)" OR title:"U.S. Marshalls (1998)" OR title:"Stranger, The (1994)" OR title:"Tank Girl (1995)" OR title:"Men With Guns (1997)" OR title:"Deep Rising (1998)" OR title:"Abyss, The (1989)" OR title:"Tokyo Fist (1995)" OR title:"Ben-Hur (1959)" OR title:"Aliens (1986)" OR title:"No Escape (1994)" OR title:"Dead Presidents (1995)" OR title:"Lost World: Jurassic Park, The (1997)" OR title:"Set It Off (1996)" OR title:"Ghost and the Darkness, The (1996)" OR title:"Substitute, The (1996)" OR title:"Star Trek IV: The Voyage Home (1986)" OR title:"Batman (1989)" OR title:"Event Horizon (1997)" OR title:"Stargate (1994)" OR title:"Star Trek III: The Search for Spock (1984)" OR title:"Coldblooded (1995)" OR title:"Raiders of the Lost Ark (1981)" OR title:"Muppet Treasure Island (1996)" OR title:"Batman Forever (1995)" OR title:"Sudden Death (1995)" OR title:"Terminator, The (1984)" OR title:"American Strays (1996)" OR title:"Last Man Standing (1996)" OR title:"Replacement Killers, The (1998)" OR title:"Cowboy Way, The (1994)" OR title:"Glimmer Man, The (1996)" OR title:"Man in the Iron Mask, The (1998)" OR title:"Godfather, The (1972)" OR title:"Demolition Man (1993)" OR title:"Three Musketeers, The (1993)" OR title:"Lost in Space (1998)" OR title:"Last Action Hero (1993)" OR title:"Hunt for Red October, The (1990)" OR title:"Executive Decision (1996)" OR title:"Crow: City of Angels, The (1996)" OR title:"Blown Away (1994)" OR title:"Smilla's Sense of Snow (1997)" OR title:"Conspiracy Theory (1997)" OR title:"Evil Dead II (1987)" OR title:"Crow, The (1994)" OR title:"Shooter, The (1995)" OR title:"Starship Troopers (1997)" OR title:"Fallen (1998)" OR title:"First Knight (1995)" OR title:"Fugitive, The (1993)" OR title:"Transformers: The Movie, The (1986)" OR title:"Young Guns (1988)" OR title:"Bird of Prey (1996)" OR title:"Jaws 3-D (1983)" OR title:"G.I. Jane (1997)" OR title:"Terminal Velocity (1994)" OR title:"Jurassic Park (1993)" OR title:"Mirage (1995)" OR title:"Adventures of Robin Hood, The (1938)" OR title:"Steel (1997)" OR title:"Blues Brothers, The (1980)" OR title:"Hunted, The (1995)" OR title:"Die Hard: With a Vengeance (1995)" OR title:"Desperado (1995)" OR title:"Get Shorty (1995)" OR title:"Braveheart (1995)" OR title:"3 Ninjas: High Noon At Mega Mountain (1998)" OR title:"Return of the Jedi (1983)" OR title:"Under Siege 2: Dark Territory (1995)" OR title:"Street Fighter (1994)" OR title:"Program, The (1993)" OR title:"Devil's Own, The (1997)" OR title:"True Lies (1994)" OR title:"Mission: Impossible (1996)" OR title:"Mars Attacks! (1996)" OR title:"Menace II Society (1993)" OR title:"Clear and Present Danger (1994)" OR title:"U Turn (1997)" OR title:"Peacemaker, The (1997)" OR title:"Highlander (1986)" OR title:"Magnificent Seven, The (1954)" OR title:"Escape from L.A. (1996)" OR title:"Pagemaster, The (1994)" OR title:"Next Karate Kid, The (1994)" OR title:"I Love Trouble (1994)" OR title:"Striking Distance (1993)" OR title:"Mortal Kombat (1995)" OR title:"Perfect World, A (1993)" OR title:"Waterworld (1995)" OR title:"Titanic (1997)" OR title:"Beverly Hills Ninja (1997)" OR title:"Money Train (1995)" OR title:"Saint, The (1997)" OR title:"Money Talks (1997)" OR title:"Judgment Night (1993)" OR title:"Time Tracers (1995)" OR title:"Heat (1995)" OR title:"Fled (1996)" OR title:"Cyrano de Bergerac (1990)" OR title:"Lashou shentan (1992)" OR title:"Double Team (1997)" OR title:"Twister (1996)" OR title:"Marked for Death (1990)" OR title:"Mad City (1997)" OR title:"Butch Cassidy and the Sundance Kid (1969)" OR title:"Drop Zone (1994)" OR title:"Shopping (1994)" OR title:"Highlander III: The Sorcerer (1994)" OR title:"Quest, The (1996)" OR title:"Conan the Barbarian (1981)" OR title:"Hard Target (1993)" OR title:"Jumanji (1995)" OR title:"Best of the Best 3: No Turning Back (1995)" OR title:"Tough and Deadly (1995)" OR title:"Jerky Boys, The (1994)" OR title:"Supercop (1992)" OR title:"GoldenEye (1995)" OR title:"Spawn (1997)" OR title:"Getaway, The (1994)" OR title:"Blood Beach (1981)" OR title:"Batman Returns (1992)" OR title:"Fire Down Below (1997)" OR title:"Target (1995)" OR title:"Faster Pussycat! Kill! Kill! (1965)" OR title:"Apollo 13 (1995)" OR title:"Diva (1981)" OR title:"Arrival, The (1996)" OR title:"Barb Wire (1996)" OR title:"In the Line of Fire (1993)" OR title:"Die xue shuang xiong (Killer, The) (1989)" OR title:"Low Down Dirty Shame, A (1994)" OR title:"Bad Boys (1995)" OR title:"Speed (1994)" OR title:"Johnny 100 Pesos (1993)" OR title:"The Courtyard (1995)" OR title:"Star Trek V: The Final Frontier (1989)" OR title:"Independence Day (ID4) (1996)" OR title:"Warriors of Virtue (1997)" OR title:"Godfather: Part II, The (1974)" OR title:"Operation Dumbo Drop (1995)" OR title:"Strange Days (1995)" OR title:"Kull the Conqueror (1997)" OR title:"New York Cop (1996)" OR title:"Face/Off (1997)" OR title:"Indiana Jones and the Last Crusade (1989)" OR title:"Bulletproof (1996)" OR title:"Jackal, The (1997)" OR title:"Hot Shots! Part Deux (1993)" OR title:"Judge Dredd (1995)" OR title:"Days of Thunder (1990)" OR title:"Men in Black (1997)" OR title:"Escape from New York (1981)" OR title:"Army of Darkness (1993)" OR title:"Glory (1989)" OR title:"Men of Means (1998)" OR title:"Die Hard 2 (1990)" OR title:"Empire Strikes Back, The (1980)" OR title:"Dragonheart (1996)" OR title:"Shadow, The (1994)" OR title:"Die Hard (1988)" OR title:"River Wild, The (1994)" OR title:"Alien (1979)" OR title:"Police Story 4: Project S (Chao ji ji hua) (1993)" OR title:"From Dusk Till Dawn (1996)" OR title:"Turbo: A Power Rangers Movie (1997)" OR title:"True Romance (1993)" OR title:"Cutthroat Island (1995)" OR title:"Hard Rain (1998)" OR title:"Chain Reaction (1996)" OR title:"Star Trek: Generations (1994)" OR title:"Beverly Hills Cop III (1994)" OR title:"Johnny Mnemonic (1995)" OR title:"Condition Red (1995)" OR title:"Terminator 2: Judgment Day (1991)" OR title:"Jaws (1975)" OR title:"Jackie Chan's First Strike (1996)" OR title:"Blues Brothers 2000 (1998)" OR title:"Hackers (1995)" OR title:"Fifth Element, The (1997)" OR title:"Good, The Bad and The Ugly, The (1966)" OR title:"Batman & Robin (1997)" OR title:"Nemesis 2: Nebula (1995)" OR title:"African Queen, The (1951)" OR title:"Outbreak (1995)" OR title:"Quick and the Dead, The (1995)" OR title:"Last of the Mohicans, The (1992)" OR title:"Speed 2: Cruise Control (1997)" OR title:"Surviving the Game (1994)" OR title:"King of New York (1990)" OR title:"Under Siege (1992)" OR title:"Princess Bride, The (1987)" OR title:"Hostile Intentions (1994)" OR title:"Eraser (1996)" OR title:"Young Guns II (1990)" OR title:"Maximum Risk (1996)" OR title:"Mortal Kombat: Annihilation (1997)" OR title:"Maverick (1994)" OR title:"Lawnmower Man, The (1992)" OR title:"Full Metal Jacket (1987)" OR title:"Stag (1997)" OR title:"Super Mario Bros. (1993)" OR title:"Daylight (1996)" OR title:"Congo (1995)" OR title:"Natural Born Killers (1994)" OR title:"Heavy Metal (1981)" OR title:"Dante's Peak (1997)" OR title:"Anaconda (1997)" OR title:"Breakdown (1997)",movie_id:[* TO *]""",
      "fields" -> "movie_id,title",
      "sort" -> "id asc"
    )
    val solrConf = new SolrConf(opts)
    val filters = solrConf.getFilters
    assert(filters(0) === "genre:action")
    assert(filters(2) === "movie_id:[* TO *]")
    assert(filters.length === 3)
    val df = sparkSession.read.format("solr").options(opts).load()
    val rows = df.collectAsList()
    assert(rows.size() === 251)
  }
}
