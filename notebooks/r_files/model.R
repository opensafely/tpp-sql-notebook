library(dplyr)
data <- read.csv('C:/Users/alexw/Documents/GitHub/tpp-sql-notebook/data/analysis/final_dataset.csv')

ages <- pull(data, Age)
age.cat <- cut(ages, breaks=c(0, 40, 70, 120), right = FALSE)
data["age.cat"] <- age.cat


model <- glm(died ~ Sex + age.cat + smoking_status + chd_code,
             family=binomial(link='logit'),
             data=data
             )
exp(coef(model))
