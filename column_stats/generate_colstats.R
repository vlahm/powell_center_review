library(plyr)

#clear env and console
rm(list=ls()); cat('\014')

#suppress scientific notation
options(scipen=100)

#specify dataset location and data file name
setwd('C:\\Users\\vlahm\\Desktop\\powell_center_review\\data\\ts_data')
dataset = 'site_data'

# setup ####
#load file
x = read.table(paste0(dataset, '.tsv'), header=TRUE, sep='\t',
    stringsAsFactors=FALSE)
head(x)

# separate into numeric and character data.frames
colclasses = sapply(x, class)
num = x[,which(colclasses != 'character')]
char = x[,which(colclasses == 'character')]

# extract column stats ####
#get numeric column stats
numstats = do.call(data.frame,
    list(datatype = sapply(num, class),
        n = apply(num, 2, length),
        n_unique = apply(num, 2, function(x) length(unique(x))),
        n_duplicated = apply(num, 2, function(x) sum(duplicated(x))),
        min = round(apply(num, 2, min, na.rm=TRUE), 3),
        max = round(apply(num, 2, max, na.rm=TRUE), 3),
        mean = round(apply(num, 2, mean, na.rm=TRUE), 3),
        median = round(apply(num, 2, median, na.rm=TRUE), 3),
        sd = round(apply(num, 2, sd, na.rm=TRUE), 3),
        n_over_2sd = apply(num, 2, function(x){
            x = na.omit(x)
            sum(x > mean(x) + 2*sd(x))
        }),
        n_under_neg2sd = apply(num, 2, function(x){
            x = na.omit(x)
            sum(x < mean(x) - 2*sd(x))
        }),
        zero_count = apply(num, 2, function(x) sum(na.omit(x == 0))),
        zero_proportion = round(apply(num, 2, function(x){
            sum(na.omit(x == 0))/length(x)
        }), 3),
        neg_count = apply(num, 2, function(x) sum(na.omit(x < 0))),
        neg_proportion = round(apply(num, 2, function(x){
            sum(na.omit(x < 0))/length(x)
        }), 3),
        NA_count = apply(num, 2, function(x) sum(is.na(x))),
        NA_proportion = round(apply(num, 2, function(x){
            sum(is.na(x))/length(x)
        }), 3),
        NaN_count = apply(num, 2, function(x) sum(is.nan(x))),
        NaN_proportion = round(apply(num, 2, function(x){
            sum(is.nan(x))/length(x)
        }), 3),
        NULL_count = apply(num, 2, function(x) sum(is.null(x))),
        NULL_proportion = round(apply(num, 2, function(x){
            sum(is.null(x))/length(x)
        }), 3),
        Inf_count = apply(num, 2, function(x) sum(is.infinite(x)))))
numstats

#get character column stats
charstats = do.call(data.frame,
    list(datatype = sapply(char, class),
        n = apply(char, 2, length),
        n_unique = apply(char, 2, function(x) length(unique(x))),
        n_duplicated = apply(char, 2, function(x) sum(duplicated(x))),
        NA_count = apply(char, 2, function(x) sum(is.na(x))),
        NA_proportion = round(apply(char, 2, function(x){
            sum(is.na(x))/length(x)
        }), 3),
        NULL_count = apply(char, 2, function(x) sum(is.null(x))),
        NULL_proportion = round(apply(char, 2, function(x){
            sum(is.null(x))/length(x)
        }), 3),
        blank_count = apply(char, 2, function(x) sum(na.omit(x == ''))),
        blank_proportion = round(apply(char, 2, function(x){
            sum(na.omit(x == ''))/length(x)
        }), 3)))

charstats

#recombine frames and write output
combined = rbind.fill(list(numstats, charstats), row.names=TRUE)
rownames(combined) = c(rownames(numstats), rownames(charstats))

write.csv(combined,
    paste0('C:/Users/vlahm/Desktop/powell_center_review/column_stats/',
    dataset, '_colstats.csv'))

# check for duplicates ####
#check for duplicated rows in whole frame, num subset, char subset
dups = duplicated(x) | duplicated(x[nrow(x):1, ])[nrow(x):1]
dupsn = duplicated(num) | duplicated(num[nrow(num):1, ])[nrow(num):1]
dupsc = duplicated(char) | duplicated(char[nrow(char):1, ])[nrow(char):1]

#locate rows for all duplicates within a column
dups2 = duplicated(num$dvqcoefs.c) | rev(duplicated(rev(num$dvqcoefs.c)))
dups3 = which(dups2)[!is.na(num$dvqcoefs.c[which(dups2)])]
x[dups2,]
x[dups3,]
