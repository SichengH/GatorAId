
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


setwd("/Users/sichenghao/Desktop/Projects/Mayo_team_1/")

##### Inclusion First ICU admission, MICU#####

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


# fwrite(vaso,"vaso.csv")
# fwrite(vital,"vital.csv")
# fwrite(sapsii,"sapsii.csv")
# fwrite(rrt,"rrt.csv")
# fwrite(chem,"chem.csv")
# fwrite(cci,"cci.csv")
# fwrite(sofa,"sofa.csv")




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
k_meds_d = input_drug%>%filter(k_meds == 'i')
other_meds_d = input_drug%>%filter(other_meds == 'x')

k_meds = input%>%filter(itemid%in%k_meds_d$itemid)
k_meds = left_join(k_meds,k_meds_d%>%select(itemid,label))


k_meds = k_meds%>%filter(amount<1000)
hist(k_meds_cohort$amount)
table(k_meds$label)


View(k_meds%>%filter(label == 'KCL (Bolus)'))
View(k_meds%>%filter(label == 'KCl (CRRT)'))%>%arrange(stay_id,starttime)
View(k_meds%>%filter(label == 'Potassium Chloride'))

## point check
View(k_meds%>%filter(stay_id == '39339791'))
View(k_meds%>%filter(stay_id == '30005000')%>%arrange(starttime))

k_meds_bolus = k_meds%>%filter(label == 'KCL (Bolus)')
k_meds_crrt = k_meds%>%filter(label == 'KCl (CRRT)')
k_meds_KCL = k_meds%>%filter(label == 'Potassium Chloride')


## First Version
#k_meds_bolus$amount = k_meds_bolus$amount/2.5

k_meds_bolus$amount = ifelse(k_meds_bolus$amount>60,k_meds_bolus$amount/10,k_meds_bolus$amount/2.5)



hist(k_meds_bolus$amount,breaks = 100)

hist(k_meds_crrt$amount,breaks = 100)

hist(k_meds_KCL$amount,breaks = 100)

k_meds_new = rbind(k_meds_bolus,k_meds_crrt,k_meds_KCL)

k_meds_new = k_meds_new%>%filter(amount<100)%>%filter(stay_id%in%static$stay_id)
hist(k_meds_new$amount)


########### cleaning ############
icu_hourly = left_join(icu_hourly,static%>%select(stay_id,hadm_id))

#k_data = chem%>%filter(!is.na(potassium))
View(k_meds_new)
k_meds_new$starttime = floor_date(k_meds_new$starttime,unit = "hour")
k_meds_new$endtime = floor_date(k_meds_new$endtime,unit = "hour")

##sum meds when in the same hour
cohort = left_join(icu_hourly,k_meds_new%>%transmute(stay_id,kcl_amount = amount,time = starttime),by = c("stay_id","endtime" = "time"))


chem$endtime = floor_date(chem$charttime, unit = "hour")
chem_new = chem%>%
  group_by(hadm_id,endtime)%>%
  summarise(potassium = mean(potassium),
            sodium = mean(sodium),
            glucose = mean(glucose),
            creatinine = mean(creatinine),
            albumin = mean(albumin))%>%
  ungroup()

cohort = left_join(cohort,chem_new,by = c("hadm_id","endtime"))

fwrite(cohort,file = "cohort_new_v1.csv")
fwrite(static,file = "static_var_v1.csv")

########## old code #########



cohort = cohort[,-c(1,2)]
cohort_meds = full_join(cohort,k_meds_new%>%transmute(stay_id,label,amount,charttime = starttime))

rrt_activate = rrt%>%filter(dialysis_active==1)%>%filter(stay_id%in%static$stay_id)
cohort_rrt = full_join(cohort_meds,rrt_activate)

cohort_bg = full_join(cohort_rrt,bg)
cohort_vaso = full_join(cohort_bg,vaso%>%
                          transmute(stay_id,charttime = starttime,vaso_binary))
cohort_cardiac = full_join(cohort_vaso,cardiac)

fwrite(static,"cohort(raw).csv")

temp = cohort_cardiac%>%
  group_by(stay_id)%>%
  arrange(stay_id,charttime)%>%
  mutate(row_num = row_number()) %>%
  ungroup()

temp[is.na(temp$potassium),]$row_num = NA

View(temp%>%arrange(stay_id,charttime))

temp = temp%>%
  arrange(stay_id,charttime)%>%
  fill(row_num, .direction = "up")


data = temp%>%
  group_by(stay_id,row_num)%>%
  summarise(time = min(charttime),
            #gender = max(gender),
            #insurance = max(insurance),
            #age = max(age),
            #hospital_expire_flag = max(hospital_expire_flag),
            #mort30 = mort30,
            #ED = max(ED),
            potassium = max(potassium,na.rm = T),
            amount = sum(amount,na.rm = T),
            sodium = max(sodium,na.rm = T),
            glucose = max(glucose,na.rm = T),
            creatinine = max(creatinine,na.rm = T),
            chloride = max(chloride,na.rm = T),
            calcium = max(calcium,na.rm = T),
            dialysis_active = max(dialysis_active,na.rm = T),
            ph = max(ph,na.rm = T),
            vaso_binary = max(vaso_binary,na.rm = T),
            troponin_t = max(troponin_t,na.rm = T),)

data_all = left_join(data,static)


data_all_temp = data_all%>%filter(amount<50)%>%filter(amount>0)
hist(data_all_temp$amount,breaks = 100,
     ylab = "Frequency of replacement",
     xlab = "KCL amount(mEq)",
     main = "Histogram ")
fwrite(data_all,"cohort_v3.csv")


                                              