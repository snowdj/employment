# devtools::install_version("haven", version = "1.1.0", repos = "http://cran.us.r-project.org")
# raw_2016 <- haven::read_sav("~/Documents/APSP_JD16_CLIENT_PWTA16.sav")

# devtools::use_data(raw_2016)

# raw_2011 <- haven::read_sav("~/projects/employment/APS2011.sav")
# raw_2011 <- eeemployment::raw_2011
# names(raw_2011) <- toupper(names(raw_2011))
# devtools::use_data(raw_2011, overwrite = TRUE)

#
# raw_2012 <- haven::read_sav("~/projects/employment/APS2012.sav")
# devtools::use_data(raw_2012)
#
# raw_2013 <- haven::read_sav("~/projects/employment/APS2013.sav")
# devtools::use_data(raw_2013)
#
# raw_2014 <- haven::read_sav("~/projects/employment/APS2014.sav")
# devtools::use_data(raw_2014)
#
# raw_2015 <- haven::read_sav("~/projects/employment/APS2015.sav")
# devtools::use_data(raw_2015)
#
# raw_2017 <- haven::read_sav("~/projects/employment/APSP_JD17_CLIENT_PWTA17.sav")
# devtools::use_data(raw_2017)


# load_all() will load the raw data every time you run it without reset = FALSE
devtools::load_all(reset = FALSE)

# raw_2016 <- eeemployment::raw_2016

# both the sic lists in spss are identical - and between programs i think they are identical
sics <- c(1820, 2611, 2612, 2620, 2630, 2640, 2680, 3012, 3212, 3220, 3230, 4651, 4652, 4763, 4764, 4910, 4932, 4939, 5010, 5030, 5110, 5510, 5520, 5530, 5590, 5610, 5621, 5629, 5630, 5811, 5812, 5813, 5814, 5819, 5821, 5829, 5911, 5912, 5913, 5914, 5920, 6010, 6020, 6110, 6120, 6130, 6190, 6201, 6202, 6203, 6209, 6311, 6312, 6391, 6399, 6820, 7021, 7111, 7311, 7312, 7410, 7420, 7430, 7711, 7721, 7722, 7729, 7734, 7735, 7740, 7911, 7912, 7990, 8230, 8551, 8552, 9001, 9002, 9003, 9004, 9101, 9102, 9103, 9104, 9200, 9311, 9312, 9313, 9319, 9321, 9329, 9511, 9512)

# I dont understand why there is a value of 0 in NEWNAT for eu and age - I'm  just gonna leave it for now - the row is set as missing in my data
# can't work out why ethnicity figures don't match

raw_subset_2016 <- raw_2011 %>%
  dplyr::select(
    INDC07M,
    INDC07S,
    SOC10M,
    SOC10S,
    INECAC05,
    SECJMBR,
    PWTA14,
    INDSC07M,
    INDSC07S,
    SEX,
    AGES,
    AGE,
    ETHUK11,
    #HIQUL15D,
    FTPT,
    NSECMJ10,
    SECTRO03,
    GORWKR,
    GORWK2R)

# part 1 - update categories

# part 2 - created weighted count just say use weight col if sic in siclist

# part 3 - need to calculate main and second job individually, split by e/se
# can we just add both together first? so employed, keep weight if INECAC05 = 1 and SECJMBR = 1.

# part 4 - group by sic and category


#### REASONS TO USE PYTHON OVER R
# In R, because of the inefficient memory use, you have to aggregate the data before mapping on sector etc
# 3 way contingency tables are confusing in R, easy in python: http://wesmckinney.com/blog/contingency-tables-and-cross-tabulations-in-pandas/
# R does copy on change, whereas pandas works by reference so is much faster and uses memory more efficiently. for example, rerunning the code multiple times as is common when change formatting at the end of the process will be much better in python, and in R I have come across memory problems when dealing with the sic level dataset which has forced me to change the strucutre of the code to something less intuitive.
# fundamentally, R is designed for statistical analysis, not automation, whereas python is a general purpose language which is widely used for automation, e.g. luigi, HR etc.


# the problem with adding together the main and second job, is you then loose which part of the count comes from which sic.

# remember that we need a separate "all dcms" sector, because there is overlap so can't just add them up

# REMEMBER - mapping sics have decimal, list of sics dont
rm(list = ls()[!(ls() %in% c("raw_subset_2016", "sics"))])
# make columns
df <- as.data.frame(raw_subset_2016)
#write.csv(df, "~/data/raw_subset_2016.csv")
df$SECJMBR <- ifelse(df$SECJMBR == 3, 1, df$SECJMBR)

df$cs_flag <- ifelse(df$SECTRO03 != 7 | is.na(df$SECTRO03), 0, 1)

# format columns
# age - ages is a code (1-15) for the age bands. We want to define our own categories so use age column
# cut will do (16, 25] for 0, 16, 25, so need to do 0, 15, 24
df$dcms_ageband <-
  as.character(
    cut(as.numeric(df$AGE),
        breaks=c(-1, 15, 24, 39, 59, Inf),
        labels=c("0-15 years", "16-24 years", "25-39 years", "40-59 years", "60 years +")
    )
  )

# df$dcms_ageband <-
#   as.character(
#     cut(as.numeric(df$AGE),
#         breaks=c(-1, 15, 19, 24, 29, 34, 39, 44, 49, 54, 59, 64, 69, Inf),
#         labels=c("0-15 years", "16-19 years", "20-24 years", "25-29 years", "30-34 years", "35-39 years", "40-44 years", "45-49 years", "50-54 years", "55-59 years", "60-64 years", "65-69 years", "70 and over")
#     )
#   )

df$NewAge <- ifelse(df$AGE < 30, 29, 30)

df$sex <- as.integer(haven::zap_labels(df$SEX))
df$sex <- ifelse(df$sex == 1, "Male", "Female")

df$ethnicity <- as.integer(haven::zap_labels(df$ETHUK1))
# unique(df$ethnicity)
# df$ETHUK11 # displays label mappings - travellers are mapped to 5 which is then relabelled as other.
# even though code 0 is labelled as missing (and there are 225 NA rows), no missing is output to excel
# sum(is.na(df$ETHUK11))
# Recode ETHUK11
# (1=1)
# (3=2)
# (4=3) (5=3) (6=3) (7=3) (8=3)
# (9=4)
# (10=5) (11=5) (2=5) into ETHUK11.
# I couldn't work it out from the SPSS code, but judging by the numbers produced, missing ethnicities are included in BAME
df$ethnicity <- ifelse(df$ethnicity != 1 | is.na(df$ethnicity), "BAME", "White")

#we drop missing and don't know
# df$qualification <- haven::as_factor(df$HIQUL15D)
# levels(df$qualification)[levels(df$qualification)=="Degree or equivalent"] <- "Degree or equivalent"
# levels(df$qualification)[levels(df$qualification)=="Higher education"] <- "Higher Education"
# levels(df$qualification)[levels(df$qualification)=="GCE A level or equivalent"] <- "A Level or equivalent"
# levels(df$qualification)[levels(df$qualification)=="GCSE grades A*-C or equivalent"] <- "GCSE A* - C or equivalent"
# levels(df$qualification)[levels(df$qualification)=="Other qualification"] <- "Other"
# levels(df$qualification)[levels(df$qualification)=="No qualification"] <- "No Qualification"
# levels(df$qualification)[levels(df$qualification)=="Don?t know"] <- "dont know"
# df$qualification <- as.character(df$qualification)

# it looks like everything except full time and part time is dropped since the vlookup in 2016 main workbook only looks for  those two values
df$ftpt <- as.character(haven::as_factor(df$FTPT))

df$nssec <- as.integer(df$NSECMJ10)
df$nssec <- ifelse(df$nssec %in% 1:4, "More Advantaged Group (NS-SEC 1-4)", df$nssec)
df$nssec <- ifelse(df$nssec %in% 5:8, "Less Advantaged Group (NS-SEC 5-8)", df$nssec)

write.csv(df, "~/data/cleaned_2011_df.csv", row.names = FALSE)

# end of data generation for python
