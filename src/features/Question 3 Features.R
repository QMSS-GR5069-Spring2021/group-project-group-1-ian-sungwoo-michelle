# Databricks notebook source
constructorMerge <- left_join(constructors, constructorResults, by = "constructorId")
constructorMerge <- left_join(constructorMerge, constructorStandings, by = "constructorId")
constructorMerge <- constructorMerge %>%
  select(-raceId.x, -points.x) %>%
  rename(raceId = raceId.y,
        points = points.y)
constructorMerge <- left_join(constructorMerge, races, by = "raceId")
constructorMerge <- constructorMerge %>%
  filter(year >= 1950, year <= 2010) %>%
  select(wins, position, points, nationality)
constructorMerge <- constructorMerge[complete.cases(constructorMerge), ]

# COMMAND ----------

