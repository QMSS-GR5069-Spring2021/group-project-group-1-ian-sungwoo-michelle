library(tidyverse)
library(lubridate)
library(stargazer)

standing <- read_csv("C:/Users/miche/Dropbox/My PC (DESKTOP-HH7L0K3)/Desktop/f1/driver_standings.csv")
races <- read_csv("C:/Users/miche/Dropbox/My PC (DESKTOP-HH7L0K3)/Desktop/f1/races.csv")
drivers <- read_csv("C:/Users/miche/Dropbox/My PC (DESKTOP-HH7L0K3)/Desktop/f1/drivers.csv")

constructor_standing <- read_csv("C:/Users/miche/Dropbox/My PC (DESKTOP-HH7L0K3)/Desktop/f1/constructor_standings.csv")
constructor <- read_csv("C:/Users/miche/Dropbox/My PC (DESKTOP-HH7L0K3)/Desktop/f1/constructors.csv")

## driver

races$date <- as.Date(races$date)

joined <- standing %>%
  left_join(races %>% select(raceId, date), by = "raceId") %>%
  arrange(driverId, desc(date)) %>%
  select(raceId, driverId, date, position, wins, points) %>%
  mutate(position_prev = position) %>%
  left_join(drivers, by = "driverId") %>%
  select(-surname, -code, -forename, -number, -driverRef, - url)

joined$position_prev <- c(joined$position[-(seq(1))], NA)

joined <- joined %>%
  group_by(driverId) %>%
  filter(date != min(date))

joined$age <- as.numeric(difftime(joined$date, joined$dob, "days")/365)

joined$improve <- ifelse(joined$position < joined$position_prev, 1, 0)

joined$improve_new <- ifelse(joined$improve == 1, 1,
                              ifelse(joined$position == 1, 1, 0))

glm1 <- glm(improve ~ age, data = joined, family = "binomial")
glm2 <- glm(improve ~ age + driverId, data = joined, family = "binomial")
glm3 <- glm(improve ~ age + driverId + position_prev, data = joined, family = "binomial")
glm4 <- glm(improve ~ age + driverId + position_prev + wins, data = joined, family = "binomial")
glm5 <- glm(improve ~ age + driverId + position_prev + wins + points, data = joined, family = "binomial")

stargazer(glm1, glm2, glm3, glm4, glm5,
          type = "text")

## constructor

joined_constructor <- constructor_standing %>%
  left_join(constructor, by = "constructorId") %>%
  left_join(races, by = "raceId") %>%
  select(raceId, constructorId, position, wins, nationality, date, points) %>%
  arrange(constructorId, desc(date))

joined_constructor$position_prev <- c(joined_constructor$position[-(seq(1))], NA)

joined_constructor <- joined_constructor %>%
  group_by(constructorId) %>%
  filter(date != min(date))

joined_constructor$improve <- ifelse(joined_constructor$position < joined_constructor$position_prev, 1, 0)

joined_constructor$improve_new <- ifelse(joined_constructor$improve == 1, 1,
                             ifelse(joined_constructor$position == 1, 1, 0))


glm1 <- glm(improve ~ constructorId, data = joined_constructor, family = "binomial")
glm2 <- glm(improve ~ constructorId + position_prev, data = joined_constructor, family = "binomial")
glm3 <- glm(improve ~ constructorId + position_prev + wins, data = joined_constructor, family = "binomial")
glm4 <- glm(improve ~ constructorId + position_prev + wins + points, data = joined_constructor, family = "binomial")

stargazer(glm1, glm2, glm3, glm4,
          type = "text")

# -------------------


lm1 <- lm(improve ~ age, data = joined)
lm3 <- lm(improve ~ age + position_prev, data = joined )
lm4 <- lm(improve ~ age + position_prev + wins, data = joined )
lm5 <- lm(improve ~ age + position_prev + wins + points, data = joined )

stargazer(lm1, lm3, lm4, lm5,
          type = "text")

lm1 <- lm(improve ~ constructorId, data = joined_constructor )
lm2 <- lm(improve ~ constructorId + position_prev, data = joined_constructor )
lm3 <- lm(improve ~ constructorId + position_prev + wins, data = joined_constructor )
lm4 <- lm(improve ~ constructorId + position_prev + wins + points, data = joined_constructor )

stargazer(lm1, lm2, lm3, lm4,
          type = "text")


