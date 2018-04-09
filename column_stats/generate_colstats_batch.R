library(plyr)

#clear env and console
rm(list=ls()); cat('\014')

#suppress scientific notation
options(scipen=100)

#specify dataset location and data file name
# setwd('C:\\Users\\vlahm\\Desktop\\powell_center_review\\data\\ts_data')

#get sb data
# install.packages('devtools')
# library(devtools)
# install_github("USGS-R/sbtools")
library(sbtools)
library(sourcetools)
library(stringr)

#load sb credentials and local directory locations
conf = read_lines('/home/mike/git/streampulse/server_copy/sp/config.py')
extract_from_config = function(key){
    ind = which(lapply(conf, function(x) grepl(key, x)) == TRUE)
    val = str_match(conf[ind], '.*\\"(.*)\\"')[2]
    return(val)
}
sb_usr = extract_from_config('SB_USER')
sb_pass = extract_from_config('SB_PASS')

#log in to sciencebase
authenticate_sb(sb_usr, sb_pass)
Sys.sleep(5)




#get objects in metadata folder on sb
sb_meta_children = item_list_children('59c03b72e4b091459a5e0b64', fields='id', limit=99999)
insb_meta = vector()
for(i in 1:length(sb_meta_children)){
    sb_meta_obj = item_get(sb_meta_children[[i]]$id)
    sb_meta_obj = item_get(sb_meta_children[[1]]$id)
    for(j in 1:length(sb_meta_obj$files)){
        insb_meta = append(insb_meta, sb_meta_obj$files[[j]]$name)
        insb_meta = append(insb_meta, sb_meta_obj$files[[1]]$name)
        sb_meta_obj$files
        tsworkpath = '~/git/powell_center_review/data/ts_data/set'
        dir.create(paste0(tsworkpath, i))
        item_file_download(sb_meta_obj, dest_dir=paste0(tsworkpath, i))
        zip_file = list.files(paste0(tsworkpath, i), pattern='.*zip')
        unzipped = unzip(zipfile=paste0(tsworkpath, i, '/', zip_file),
            exdir=paste0(tsworkpath, i))

    }
}
getwd()

    unzipped <- unzip(zipfile=zip.file, exdir=file.path(tempdir(), 'config'))
    data_df <- readr::read_tsv(unzipped)



files = list.files()

combined = data.frame()
for(f in 1:length(files)){

    print(paste(f, files[f]))
    dataset = files[f]
    # dataset = files[1]

    # setup ####
    #load file
    x = read.table(paste0(dataset), header=TRUE, sep='\t',
        stringsAsFactors=FALSE)#, colClasses=c('POSIXct','numeric'))

    head(x)

    #determine which columns contain dates and datetimes
    datecols = apply(x, 2, function(z){
        grepl('[0-9]{4}.[0-9]{2}.[0-9]{2}$', z[1])
    })
    if(any(datecols)){
        x[,datecols] = lapply(x[,datecols, drop=FALSE], as.Date)
    }

    posixcols = apply(x, 2, function(z){
        grepl('[0-9]{4}.[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{2}.[0-9]{2}$', z[1])
        # grepl('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$', z)
    })
    if(any(posixcols)){
        x[,posixcols] = lapply(x[,posixcols, drop=FALSE], function(z){
            as.POSIXct(z, format='%Y-%m-%d %H:%M:%S')
        })
    }

    print(str(x))

    # separate into numeric, character, datetime data.frames
    colclasses = lapply(x, class)
    num = x[,which(colclasses %in% c('numeric', 'integer')), drop=FALSE]
    char = x[,which(colclasses == 'character'), drop=FALSE]
    dtcols = vector()
    for(i in 1:length(colclasses)){
        if('POSIXct' %in% colclasses[[i]] | colclasses[i] == 'Date')
            dtcols = append(dtcols, i)
    }
    datetime = x[,dtcols, drop=FALSE]

    # extract column stats ####
    #get numeric column stats
    numstats = do.call(data.frame,
        list(datatype = sapply(num, class),
            n = apply(num, 2, length),
            n_unique = apply(num, 2, function(x) length(unique(x))),
            n_duplicated = apply(num, 2, function(x) sum(duplicated(x))),
            min = apply(num, 2, function(x){
                as.character(round(min(x, na.rm=TRUE), 3))
            }),
            max = apply(num, 2, function(x){
                as.character(round(max(x, na.rm=TRUE), 3))
            }),
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

    #get datetime column stats
    dtstats = do.call(data.frame,
        list(datatype = sapply(datetime, function(x) class(x)[1]),
            n = apply(datetime, 2, length),
            n_unique = apply(datetime, 2, function(x) length(unique(x))),
            n_duplicated = apply(datetime, 2, function(x) sum(duplicated(x))),
            min = apply(datetime, 2, function(x){
                as.character(min(x, na.rm=TRUE))
            }),
            max = apply(datetime, 2, function(x){
                as.character(max(x, na.rm=TRUE))
            }),
            # max = apply(datetime, 2, max, na.rm=TRUE),
            zero_count = apply(datetime, 2, function(x) sum(na.omit(x == 0))),
            zero_proportion = round(apply(datetime, 2, function(x){
                sum(na.omit(x == 0))/length(x)
            }), 3),
            neg_count = apply(datetime, 2, function(x) sum(na.omit(x < 0))),
            neg_proportion = round(apply(datetime, 2, function(x){
                sum(na.omit(x < 0))/length(x)
            }), 3),
            NA_count = apply(datetime, 2, function(x) sum(is.na(x))),
            NA_proportion = round(apply(datetime, 2, function(x){
                sum(is.na(x))/length(x)
            }), 3),
            NaN_count = apply(datetime, 2, function(x) sum(is.nan(x))),
            NaN_proportion = round(apply(datetime, 2, function(x){
                sum(is.nan(x))/length(x)
            }), 3),
            NULL_count = apply(datetime, 2, function(x) sum(is.null(x))),
            NULL_proportion = round(apply(datetime, 2, function(x){
                sum(is.null(x))/length(x)
            }), 3),
            Inf_count = apply(datetime, 2, function(x) sum(is.infinite(x))),
            mode_interval_mins = apply(datetime, 2, function(x){
                intervals = rle(diff(as.numeric(as.POSIXct(x))))
                interval_counts = tapply(intervals$lengths, intervals$values, sum)
                as.numeric(names(which.max(interval_counts))) / 60
            }),
            n_intervals = apply(datetime, 2, function(x){
                length(unique(diff(as.numeric(as.POSIXct(x)))))
            }),
            max_interval_mins = apply(datetime, 2, function(x){
                max(rle(diff(as.numeric(as.POSIXct(x))))$values) / 60
            }),
            min_interval_mins = apply(datetime, 2, function(x){
                min(rle(diff(as.numeric(as.POSIXct(x))))$values) / 60
            })))
    dtstats

    #recombine type frames and append to main frame
    combined_types = rbind.fill(list(dtstats, numstats, charstats),
        row.names=TRUE)
    combined_types = cbind(data.frame(var=c(rownames(dtstats),
            rownames(numstats), rownames(charstats))),
        data.frame(fileno=rep(f, nrow(combined_types))),
        combined_types)

    combined = rbind.fill(list(combined, combined_types), row.names=TRUE)

}

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
