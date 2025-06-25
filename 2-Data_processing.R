
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

setwd("/Users/hsc/Github/GatorAId/")


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
cohort = left_join(icu_hourly,k_meds_new%>%transmute(stay_id,hadm_id,kcl_amount = amount,time = starttime),by = c("stay_id","endtime" = "time","hadm_id"))


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

################### Exclusion Criteria ##################

length(unique(cohort$stay_id))#32562 Before exclusion

## 1. Not full code
not_full_code = code_status%>%filter(value!="Full code")
nf_id = unique(not_full_code$stay_id)

cohort_full_code = cohort%>%filter(stay_id%!in%nf_id)
length(unique(cohort_full_code$stay_id)) ## 29536

## 2. RRT
rrt_activate = rrt%>%filter(dialysis_active == 1)
rrt_present = rrt%>%filter(dialysis_present == 1)

rrt_active_id = unique(rrt_activate$stay_id)
rrt_present_id = unique(rrt_present$stay_id)

cohort_new = cohort_full_code%>%filter(stay_id%!in%rrt_present_id)
length(unique(cohort_new$stay_id)) ## 26760

## 3. sarcoridosis


## 4. remove pregnancy 


## 5. Chronic Kidney disease (CRF stage IV)

#N18.4 – Chronic kidney disease, stage 4 (severe)
#585.4 – Chronic kidney disease, Stage IV (severe)

#Remove this part

######### Adding vitals and labs ###########

#### vitals
vital_cohort = vital%>%filter(stay_id%in%cohort_new$stay_id)
vital_cohort$charttime = floor_date(vital_cohort$charttime, unit = "hour")
vital_cohort = vital_cohort%>%
  group_by(stay_id,charttime)%>%
  summarise(hr = mean(heart_rate),
            sbp = mean(sbp),
            dbp = mean(dbp),
            mbp = mean(mbp),
            rr = mean(resp_rate),
            temp = mean(temperature),
            spo2 = mean(spo2),
            glucose = mean(glucose))


#### bg



#### cbc



#### 
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


                                              