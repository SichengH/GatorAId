
library(data.table)
library(ggplot2)
library(data.table)
library(tidyr)
library(tidyverse)
library(readxl)
library(gdata)
library(lubridate)
library(comorbidity)

library(table1)

library(bigrquery)
library(tidyr)
library(DBI)
library(dbplyr)
library(dplyr)
library(broom)
library(patchwork)
library(writexl)

#setwd("/Users/sh/SCCM_T5")
#setwd("/Users/sichenghao/Desktop/Projects/Extubation_Prediction/")

projectid = "mort-prediction-icu"#replace with your own project id

bigrquery::bq_auth()#login with google account associated with physionet account

`%!in%` <- function(x, table) {
  !(x %in% table)
}

setwd("/Users/sichenghao/Documents/GitHub/GatorAId/")

##### Inclusion First ICU admission, MICU#####


###icd

sql <- "
SELECT * FROM `physionet-data.mimiciv_3_1_hosp.d_icd_diagnoses`
"
bq_data <- bq_project_query(projectid, query = sql)

icd = bq_table_download(bq_data)#
icd9 = icd%>%filter(icd_version == 9)
icd10 = icd%>%filter(icd_version == 10)

icd10 = 

###Static table###

sql <- "
select icu.* ,adm.admission_location,adm.discharge_location,hospital_expire_flag,gender,insurance,pt.dod,ROW_NUMBER() OVER (PARTITION BY icu.hadm_id ORDER BY icu.intime DESC) AS row_number
from `physionet-data.mimiciv_3_1_icu.icustays` icu
left join `physionet-data.mimiciv_3_1_hosp.admissions` adm
on adm.hadm_id = icu.hadm_id
and adm.subject_id = icu.subject_id
left join `physionet-data.mimiciv_3_1_hosp.patients` pt
on adm.subject_id = pt.subject_id
"
bq_data <- bq_project_query(projectid, query = sql)

all_icus = bq_table_download(bq_data)#
nrow(all_icus)

first_icu = all_icus%>%filter(row_number==1)#65677
micu = first_icu%>%filter(first_careunit%in%c("Medical Intensive Care Unit (MICU)",
                                              "Medical/Surgical Intensive Care Unit (MICU/SICU)"))

length(unique(micu$stay_id))#32563

sql <- "
SELECT
--    ad.subject_id,
      ad.hadm_id,
      pa.gender
    -- calculate the age as anchor_age (60) plus difference between
    -- admit year and the anchor year.
    -- the noqa retains the extra long line so the 
    -- convert to postgres bash script works
    , pa.anchor_age + DATETIME_DIFF(ad.admittime, DATETIME(pa.anchor_year, 1, 1, 0, 0, 0), YEAR) AS age -- noqa: L016
FROM `physionet-data.mimiciv_3_1_hosp.admissions` ad
INNER JOIN `physionet-data.mimiciv_3_1_hosp.patients` pa
    ON ad.subject_id = pa.subject_id
;
"
bq_data <- bq_project_query(projectid, query = sql)

age = bq_table_download(bq_data) # added anchor year


static = left_join(micu,age)
static$intime <- as.POSIXct(static$intime)
static$dod <- as.POSIXct(static$dod)

static$diff_time <- as.numeric(difftime(static$dod,static$intime, units = "days"))
static$mort30 = ifelse(static$diff_time<30|static$hospital_expire_flag==1,1,0)
static$mort30 = as.factor(ifelse(is.na(static$mort30),0,static$mort30))
levels(static$mort30) <- c("Alive(30 days)", "Not alive(30 days)")
static$ED = as.factor(ifelse(static$admission_location == "EMERGENCY ROOM",1,0))


sql <- "
SELECT * FROM `physionet-data.mimiciv_3_1_icu.chartevents`
where itemid in (223758,229784,228687)
"
bq_data <- bq_project_query(projectid, query = sql)
code_status = bq_table_download(bq_data) # added anchor year

#fwrite(static,"inclusion.csv")
#fwrite(code_status,"code_status.csv")

sql <- "
SELECT * FROM `mort-prediction-icu.derived.cci`
"
bq_data <- bq_project_query(projectid, query = sql)
cci = bq_table_download(bq_data) # commodities
cci = cci%>%select(hadm_id,charlson_comorbidity_index)%>%filter(hadm_id%in%static$hadm_id)
static = left_join(static,cci)

# sql <- "
# SELECT * FROM `mort-prediction-icu.derived.sapsii`
# "
# bq_data <- bq_project_query(projectid, query = sql)
# sapsii = bq_table_download(bq_data) # sapsii



####Non-static varietals####



sql <- "
SELECT * FROM `mort-prediction-icu.derived.icustay_hourly`
"
bq_data <- bq_project_query(projectid, query = sql)
icu_hourly = bq_table_download(bq_data) # hourly table
icu_hourly = icu_hourly%>%filter(stay_id%in%static$stay_id)%>%select(stay_id,hr,endtime)


# 
# sql <- "
# SELECT * FROM `mort-prediction-icu.derived.sofa`  
# "
# bq_data <- bq_project_query(projectid, query = sql)
# sofa = bq_table_download(bq_data) # sofa


sql <- "
SELECT * FROM `mort-prediction-icu.derived.chemistry`  
"
bq_data <- bq_project_query(projectid, query = sql)
chem = bq_table_download(bq_data) # sofa



sql <- "
SELECT * FROM `mort-prediction-icu.derived.rrt` 
"
bq_data <- bq_project_query(projectid, query = sql)
rrt = bq_table_download(bq_data) # sofa


sql <- "
SELECT * FROM `mort-prediction-icu.derived.vitalsign` 
"
bq_data <- bq_project_query(projectid, query = sql)
vital = bq_table_download(bq_data) # sofa

sql <- "
SELECT * FROM `mort-prediction-icu.derived.vasoactive_agent`
"
bq_data <- bq_project_query(projectid, query = sql)
vaso = bq_table_download(bq_data) # sofa

#triponin


sql <- "
SELECT * from `mort-prediction-icu.derived.cardiac_markers` 
"
bq_data <- bq_project_query(projectid, query = sql)
cardiac = bq_table_download(bq_data) # sofa
cardiac = cardiac%>%filter(hadm_id%in%static$hadm_id)
cardiac = left_join(cardiac,static%>%select(hadm_id,stay_id))
cardiac = cardiac%>%select(stay_id,charttime,troponin_t)

#bicarb/PH
sql <- "
SELECT * FROM `mort-prediction-icu.derived.bg`
"
bq_data <- bq_project_query(projectid, query = sql)
bg = bq_table_download(bq_data) # sofa
bg = bg%>%filter(specimen == 'ART.')%>%filter(hadm_id%in%static$hadm_id)
bg = left_join(bg,static%>%select(hadm_id,stay_id))
bg = bg%>%select(stay_id,charttime,ph)


#pressures
vaso$vaso_binary = ifelse(is.na(sum(vaso$dopamine,
                                    vaso$epinephrine,
                                    vaso$norepinephrine,
                                    vaso$vasopressin,
                                    vaso$dobutamine,na.rm = T)),0,1)
vaso = vaso%>%filter(stay_id%in%static$stay_id)


fwrite(vaso,"vaso.csv")
fwrite(vital,"vital.csv")
fwrite(sapsii,"sapsii.csv")
fwrite(rrt,"rrt.csv")
fwrite(chem,"chem.csv")
fwrite(cci,"cci.csv")
fwrite(sofa,"sofa.csv")


fwrite(static,"cohort_static.csv")
fwrite(icu_hourly,"icu_hourly.csv")



sql <- "
SELECT * FROM `physionet-data.mimiciv_3_1_icu.d_items` 
where linksto = 'inputevents'
order by label
"
bq_data <- bq_project_query(projectid, query = sql)
inputevents = bq_table_download(bq_data) # sofa



#fwrite(inputevents,"input.csv")


sql <- "
SELECT * FROM `physionet-data.mimiciv_3_1_icu.inputevents`
"
bq_data <- bq_project_query(projectid, query = sql)
input = bq_table_download(bq_data) # inputevents

input_drug = fread("input_drugs.csv")

