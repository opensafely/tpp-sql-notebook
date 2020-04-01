clear
import delimited C:/Users/alexw/Documents/GitHub/tpp-sql-notebook/data/analysis/final_dataset.csv

encode sex, gen(gender)

gen ageband = .
replace ageband = 1 if age <40
replace ageband = 2 if age < 70 & age > 40
replace ageband = 3 if age >69

label define ages 1 "under 40" 2 "40 - 69" 3 "70 and over"
label values ageband ages

label define binary 1 "yes" 0 "no"
label values admitted_itu binary
label values smoking_status binary
label values died binary
label values chd_code binary

/* making model */

logistic died i.gender i.ageband i.smoking_status i.chd_code, base
