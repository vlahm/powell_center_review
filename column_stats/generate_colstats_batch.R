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

#function for accumulating column stats for a single sb zip
getcolstats = function(iter=i){

    n_duplicate_rows = finished = redos = data.frame()

    # files = unzipped$Name
    files = unzipped
    files = files[grepl('.*.tsv$', files)]

    combined_files = data.frame()
    for(f in 1:length(files)){

        fsplit = strsplit(files[f], '/')[[1]]
        shortname = fsplit[length(fsplit)]
        print(paste(f, shortname))
        dataset = files[f]
        # dataset = files[1]

        # setup ####
        #load file
        df = try(read.table(paste0(dataset), header=TRUE, sep='\t',
            stringsAsFactors=FALSE, quote=''))
        if(class(df) == 'try-error'){
            errmsg = data.frame(setno=iter, MSG='Failed to read TSV')
            combined_files = rbind.fill(list(combined_files, errmsg),
                row.names=TRUE)
            redos = rbind.fill(list(redos,
                data.frame(fileno=f, setno=iter, SBobj=cur_childname)))
            print(paste('FAILED read', iter, shortname))
            next
        }

        # head(df)
        # str(df)
        # dim(df)

        #determine which columns contain dates and datetimes
        datecols = apply(df, 2, function(z){
            grepl('[0-9]{4}[-/][0-9]{2}[-/][0-9]{2}$', z[1])
        })
        if(any(datecols)){
            df[,datecols] = lapply(df[,datecols, drop=FALSE], as.Date)
        }

        form1 = '[0-9]{4}[-/][0-9]{2}[-/][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$'
        form2 = '[0-9]{4}[-/][0-9]{2}[-/][0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$'
        # grepl('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$', z)
        posixcols1 = apply(df, 2, function(z) {grepl(form1, z[1])} )
        posixcols2 = apply(df, 2, function(z) {grepl(form2, z[1])} )
        if(any(posixcols1)){
            df[,posixcols1] = lapply(df[,posixcols1, drop=FALSE], function(x){
                as.POSIXct(x, format='%Y-%m-%d %H:%M:%S')
            })
        }
        if(any(posixcols2)){
            df[,posixcols2] = lapply(df[,posixcols2, drop=FALSE], function(x){
                as.POSIXct(x)
            })
        }

        # print(str(df))

        # separate into numeric, character, datetime data.frames
        colclasses = lapply(df, class)

        numcols = which(colclasses %in% c('numeric', 'integer'))
        num = df[,numcols, drop=FALSE]

        charcols = which(colclasses == 'character')
        char = df[,charcols, drop=FALSE]

        dtcols = vector()
        for(k in 1:length(colclasses)){
            if('POSIXct' %in% colclasses[[k]] | colclasses[k] == 'Date')
                dtcols = append(dtcols, k)
        }
        datetime = df[,dtcols, drop=FALSE]

        logicols = which(colclasses == 'logical')
        logi = df[,logicols, drop=FALSE]

        #make sure that's all the columns
        cols_labeled = sum(length(dtcols), length(numcols),
            length(charcols), length(logicols))
        if(cols_labeled != ncol(df)){
            errmsg = data.frame(setno=iter, fileno=f,
                MSG='Not all columns accounted for')
            combined_files = rbind.fill(list(combined_files, errmsg),
                row.names=TRUE)
            redos = rbind.fill(list(redos,
                data.frame(fileno=f, setno=iter, SBobj=cur_childname)))
            print(paste('FAILED cols', iter, shortname))
            next
        }

        # extract column stats

        #get numeric column stats
        numstats = try(do.call(data.frame,
            list(datatype = sapply(num, class),
                n = apply(num, 2, length),
                # n_unique = apply(num, 2, function(x) length(unique(x))),
                # n_duplicated = apply(num, 2, function(x) sum(duplicated(x))),
                prop_dupe = round(apply(num, 2, function(x){
                    dups = sum(duplicated(x) | rev(duplicated(rev(x))))
                    dups / length(x)
                }), 3),
                min = apply(num, 2, function(x){
                    as.character(round(min(x, na.rm=TRUE), 3))
                }),
                max = apply(num, 2, function(x){
                    as.character(round(max(x, na.rm=TRUE), 3))
                }),
                mean = round(apply(num, 2, mean, na.rm=TRUE), 3),
                # median = round(apply(num, 2, median, na.rm=TRUE), 3),
                sd = round(apply(num, 2, sd, na.rm=TRUE), 3),
                # n_over_2sd = apply(num, 2, function(x){
                #     x = na.omit(x)
                #     sum(x > mean(x) + 2*sd(x))
                # }),
                # n_under_neg2sd = apply(num, 2, function(x){
                #     x = na.omit(x)
                #     sum(x < mean(x) - 2*sd(x))
                # }),
                # zero_count = apply(num, 2, function(x) sum(na.omit(x == 0))),
                zero_proportion = round(apply(num, 2, function(x){
                    sum(na.omit(x == 0))/length(x)
                }), 3),
                # neg_count = apply(num, 2, function(x) sum(na.omit(x < 0))),
                neg_proportion = round(apply(num, 2, function(x){
                    sum(na.omit(x < 0))/length(x)
                }), 3),
                # NA_count = apply(num, 2, function(x) sum(is.na(x))),
                NA_proportion = round(apply(num, 2, function(x){
                    sum(is.na(x))/length(x)
                }), 3),
                NaN_count = apply(num, 2, function(x) sum(is.nan(x))),
                # NaN_proportion = round(apply(num, 2, function(x){
                #     sum(is.nan(x))/length(x)
                # }), 3),
                NULL_count = apply(num, 2, function(x) sum(is.null(x))),
                # NULL_proportion = round(apply(num, 2, function(x){
                #     sum(is.null(x))/length(x)
                # }), 3),
                Inf_count = apply(num, 2, function(x) sum(is.infinite(x))))))

        if(class(numstats) == 'try-error'){
            errmsg = data.frame(setno=iter, fileno=f,
                MSG='Failed to extract numstats')
            combined_files = rbind.fill(list(combined_files, errmsg),
                row.names=TRUE)
            redos = rbind.fill(list(redos,
                data.frame(fileno=f, setno=iter, SBobj=cur_childname)))
            print(paste('FAILED numstats', iter, shortname))
            next
        }

        #get character column stats
        charstats = try(do.call(data.frame,
            list(datatype = sapply(char, class),
                n = apply(char, 2, length),
                # n_unique = apply(char, 2, function(x) length(unique(x))),
                # n_duplicated = apply(char, 2, function(x) sum(duplicated(x))),
                prop_dupe = round(apply(char, 2, function(x){
                    dups = sum(duplicated(x) | rev(duplicated(rev(x))))
                    dups / length(x)
                }), 3),
                # NA_count = apply(char, 2, function(x) sum(is.na(x))),
                NA_proportion = round(apply(char, 2, function(x){
                    sum(is.na(x))/length(x)
                }), 3),
                NULL_count = apply(char, 2, function(x) sum(is.null(x))),
                # NULL_proportion = round(apply(char, 2, function(x){
                #     sum(is.null(x))/length(x)
                # }), 3),
                blank_count = apply(char, 2, function(x) sum(na.omit(x == ''))))))
                # blank_proportion = round(apply(char, 2, function(x){
                #     sum(na.omit(x == ''))/length(x)
                # }), 3))))

        if(class(charstats) == 'try-error'){
            errmsg = data.frame(setno=iter, fileno=f,
                MSG='Failed to extract charstats')
            combined_files = rbind.fill(list(combined_files, errmsg),
                row.names=TRUE)
            redos = rbind.fill(list(redos,
                data.frame(fileno=f, setno=iter, SBobj=cur_childname)))
            print(paste('FAILED charstats', iter, shortname))
            next
        }
        # charstats

        #get datetime column stats
        if(length(datetime)){
            dtstats = try(do.call(data.frame,
                list(datatype = sapply(datetime, function(x) class(x)[1]),
                    n = apply(datetime, 2, length),
                    # n_unique = apply(datetime, 2, function(x) length(unique(x))),
                    # n_duplicated = apply(datetime, 2, function(x) sum(duplicated(x))),
                    prop_dupe = round(apply(datetime, 2, function(x){
                        dups = sum(duplicated(x) | rev(duplicated(rev(x))))
                        dups / length(x)
                    }), 3),
                    min = apply(datetime, 2, function(x){
                        as.character(min(x, na.rm=TRUE))
                    }),
                    max = apply(datetime, 2, function(x){
                        as.character(max(x, na.rm=TRUE))
                    }),
                    # max = apply(datetime, 2, max, na.rm=TRUE),
                    # zero_count = apply(datetime, 2, function(x) sum(na.omit(x == 0))),
                    zero_proportion = round(apply(datetime, 2, function(x){
                        sum(na.omit(x == 0))/length(x)
                    }), 3),
                    # neg_count = apply(datetime, 2, function(x) sum(na.omit(x < 0))),
                    neg_proportion = round(apply(datetime, 2, function(x){
                        sum(na.omit(x < 0))/length(x)
                    }), 3),
                    # NA_count = apply(datetime, 2, function(x) sum(is.na(x))),
                    NA_proportion = round(apply(datetime, 2, function(x){
                        sum(is.na(x))/length(x)
                    }), 3),
                    NaN_count = apply(datetime, 2, function(x) sum(is.nan(x))),
                    # NaN_proportion = round(apply(datetime, 2, function(x){
                    #     sum(is.nan(x))/length(x)
                    # }), 3),
                    NULL_count = apply(datetime, 2, function(x) sum(is.null(x))),
                    # NULL_proportion = round(apply(datetime, 2, function(x){
                    #     sum(is.null(x))/length(x)
                    # }), 3),
                    Inf_count = apply(datetime, 2, function(x) sum(is.infinite(x))),
                    mode_interval_mins = apply(datetime, 2, function(x){
                        intervals = rle(diff(as.numeric(as.POSIXct(x))))
                        interval_counts = tapply(intervals$lengths, intervals$values, sum)
                        as.numeric(names(which.max(interval_counts))) / 60
                    }),
                    # n_intervals = apply(datetime, 2, function(x){
                    #     length(unique(diff(as.numeric(as.POSIXct(x)))))
                    # }),
                    max_interval_mins = apply(datetime, 2, function(x){
                        max(rle(diff(as.numeric(as.POSIXct(x))))$values) / 60
                    }),
                    min_interval_mins = apply(datetime, 2, function(x){
                        min(rle(diff(as.numeric(as.POSIXct(x))))$values) / 60
                    }))))

            if(class(dtstats) == 'try-error'){
                errmsg = data.frame(setno=iter, fileno=f,
                    MSG='Failed to extract dtstats')
                combined_files = rbind.fill(list(combined_files, errmsg),
                    row.names=TRUE)
                redos = rbind.fill(list(redos,
                    data.frame(fileno=f, setno=iter, SBobj=cur_childname)))
                print(paste('FAILED dtstats', iter, shortname))
                next
            }
        } else {
            dtstats = data.frame()
        }
        # dtstats

        #get logical column stats
        logistats = try(do.call(data.frame,
            list(datatype = sapply(logi, class),
                n = apply(logi, 2, length),
                # NA_count = apply(logi, 2, function(x) sum(is.na(x))),
                TRUE_proportion = apply(logi, 2, function(x){
                    sum(na.omit(x))/length(x)
                }),
                NA_proportion = round(apply(logi, 2, function(x){
                    sum(is.na(x))/length(x)
                }), 3),
                NULL_count = apply(logi, 2, function(x) sum(is.null(x))))))
                # NULL_proportion = round(apply(logi, 2, function(x){
                #     sum(is.null(x))/length(x)
                # }), 3),

        if(class(logistats) == 'try-error'){
            errmsg = data.frame(setno=iter, fileno=f,
                MSG='Failed to extract logistats')
            combined_files = rbind.fill(list(combined_files, errmsg),
                row.names=TRUE)
            redos = rbind.fill(list(redos,
                data.frame(fileno=f, setno=iter, SBobj=cur_childname)))
            print(paste('FAILED logistats', iter, shortname))
            next
        }
        # logistats

        #recombine type frames and append to main frame
        combined_types = rbind.fill(list(dtstats, numstats, charstats,
            logistats), row.names=TRUE)
        combined_types = cbind(data.frame(var=c(rownames(dtstats),
            rownames(numstats), rownames(charstats), rownames(logistats))),
            data.frame(fileno=rep(f, nrow(combined_types)),
                setno=rep(iter, nrow(combined_types)),
                SBobj=rep(cur_childname, nrow(combined_types))),
            combined_types)

        combined_files = rbind.fill(list(combined_files, combined_types),
            row.names=TRUE)

        n_duplicate_rows = rbind.fill(list(n_duplicate_rows,
            data.frame(fileno=f, setno=iter, SBobj=cur_childname,
                n_dupe_rows=sum(duplicated(df)))))


        finished = rbind.fill(list(finished,
            data.frame(fileno=f, filename=shortname,
                setno=iter, SBobj=cur_childname)))

    }

    # return(combined_files)
    return(list(combined_files, n_duplicate_rows, finished, redos))
}

#working with many files from sciencebase ####

#get names of all child objects on SB
# sb_meta_children = item_list_children('59c03b72e4b091459a5e0b64', #time series
# sb_meta_children = item_list_children('59eb9b9de4b0026a55ffe37c', # inputs
sb_meta_children = item_list_children('59eb9ba6e4b0026a55ffe37f', # fits
    fields='id', limit=99999)

#sort children
childnames = vector(length=length(sb_meta_children))
for(i in 1:length(sb_meta_children)){
    childnames[i] = sb_meta_children[[i]]$title
}
sb_meta_children = sb_meta_children[order(childnames)]

#unpack each and extract column stats
insb_meta = vector()
combined_sets = n_duplicate_rows = redos = finished = data.frame()
# for(i in 1:length(sb_meta_children)){
for(i in 15){
    cur_childname = sb_meta_children[[i]]$title
    print(paste(i, cur_childname))

    sb_meta_obj = try(item_get(sb_meta_children[[i]]$id))
    if(class(sb_meta_obj) == 'try-error'){
        print('reauthenticating')
        authenticate_sb(sb_usr, sb_pass)
        sb_meta_obj = item_get(sb_meta_children[[i]]$id)
    }

    # for(j in 1:length(sb_meta_obj$files)){
    insb_meta = append(insb_meta, sb_meta_obj$files[[1]]$name)
    tsworkpath = '~/git/powell_center_review/data/fit_data/set'
    dir.create(paste0(tsworkpath, i))
    tryresp = try(item_file_download(sb_meta_obj,
        dest_dir=paste0(tsworkpath, i)))
    if(class(tryresp) == 'try-error'){
        combined_sets = rbind.fill(list(combined_sets,
            data.frame(setno=i, SBobj=cur_childname, MSG='SBerr')),
            row.names=TRUE)
        print('SB FAIL')
        next
    }

    zip_file = list.files(paste0(tsworkpath, i), pattern='.*zip')
    # unzipped = vector()
    # for(z in 1:length(zip_file)){
    unzipped = unzip(zipfile=paste0(tsworkpath, i, '/', zip_file[1]),
        exdir=paste0(tsworkpath, i))
        # unz = unzip(zipfile=paste0(tsworkpath, i, '/', zip_file[z]),
        #     exdir=paste0(tsworkpath, i))
        # unzipped = append(unz, unzipped)
    # }

    # setstats = getcolstats()
    out = getcolstats()
    setstats = out[[1]]
    n_duplicate_rows_new = out[[2]]
    finished_new = out[[3]]
    redos_new = out[[4]]

    write.csv(setstats,
        paste0(tsworkpath, i, '/fit_colstats', i, '.csv'), row.names=FALSE)

    combined_sets = rbind.fill(list(combined_sets, setstats))
        # row.names=TRUE)
    n_duplicate_rows = rbind.fill(list(n_duplicate_rows, n_duplicate_rows_new))
    finished = rbind.fill(list(finished, finished_new))
    redos = rbind.fill(list(redos, redos_new))
}

write.csv(n_duplicate_rows,
    paste0('~/git/powell_center_review/column_stats/',
        'fit_duperows.csv'), row.names=FALSE)
write.csv(combined_sets,
    paste0('~/git/powell_center_review/column_stats/',
        'fit_colstats.csv'), row.names=FALSE)
        # 'ts_colstatsALL.csv'), row.names=FALSE)
        # dataset, '_colstats.csv'))

#working with one file stored locally ####
unzipped = '/home/mike/git/powell_center_review/data/diagnostics/diagnostics.tsv'
unzipped = '/home/mike/git/powell_center_review/data/config/config.tsv'
unzipped = '/home/mike/git/powell_center_review/data/preds/daily_predictions.tsv'
out = getcolstats(1)
write.csv(out[[1]],
    paste0('~/git/powell_center_review/column_stats/',
        'preds_colstats.csv'), row.names=FALSE)
write.csv(out[[2]],
    paste0('~/git/powell_center_review/column_stats/',
        'preds_duperows.csv'), row.names=FALSE)

#part 2: summarize the summary ####
cs = read.csv('~/git/powell_center_review/column_stats/input_colstats.csv',
    stringsAsFactors=FALSE)
# cs = combined_sets

issues = data.frame()
for(i in 1:length(unique(cs$setno))){
    setind = cs$setno == i
    print(i)

    issues = rbind.fill(list(issues,
        data.frame(
            setno=i,
            setname=cs$SBobj[setind][1],
            nfiles=length(unique(cs$fileno[setind])),
            norows=sum(setind & cs$n == 0, na.rm=TRUE),
            onerow=sum(setind & cs$n == 1, na.rm=TRUE),
            # hasdupes=sum(setind & cs$prop_dupe > 0, na.rm=TRUE),
            haszeros=sum(setind & cs$zero_proportion > 0, na.rm=TRUE),
            allzero=sum(setind & cs$zero_proportion == 1, na.rm=TRUE),
            hasNA=sum(setind & cs$NA_proportion > 0, na.rm=TRUE),
            allNA=sum(setind & cs$zero_proportion == 1, na.rm=TRUE),
            hasnegs=sum(setind & cs$neg_proportion > 0, na.rm=TRUE),
            allnegs=sum(setind & cs$neg_proportion == 1, na.rm=TRUE),
            hasInf=sum(setind & cs$Inf_count > 0, na.rm=TRUE),
            hasblank=sum(setind & cs$blank_count > 0, na.rm=TRUE),
            has_dupe_timestamps=sum(setind & cs$min_interval_mins == 0,
                na.rm=TRUE),
            alltrue=sum(setind & cs$TRUE_proportion == 1, na.rm=TRUE),
            allfalse=sum(setind & cs$TRUE_proportion == 0, na.rm=TRUE),
            no_file_err=sum(setind & cs$MSG == 'SBerr', na.rm=TRUE))))
}
dim(issues)

write.csv(issues,
    paste0('~/git/powell_center_review/column_stats/',
        'input_colstat_summary.csv'), row.names=FALSE)

#fix dupe summaries
workingdir = '~/git/powell_center_review/data/input_data'
n_duplicate_rows = data.frame()
for(i in 1:length(list.dirs(workingdir))){
    print(i)
    fs = list.files(paste0(workingdir, '/set', i))
    targetfile = fs[grepl('.*\\.tsv?', fs)]
    df = read.table(paste0(workingdir, '/set', i, '/', targetfile),
        sep='\t', stringsAsFactors=FALSE, header=TRUE)
    n_duplicate_rows = rbind.fill(list(n_duplicate_rows,
        data.frame(setno=i, SBobj=cur_childname, filename=targetfile,
            n_dupe_rows=sum(duplicated(df)))))
}

write.csv(n_duplicate_rows,
    paste0('~/git/powell_center_review/column_stats/',
        'input_duperows.csv'), row.names=FALSE)

# zz = read.table('~/git/powell_center_review/data/fit_data/set2/daily.tsv',
#     sep='\t', stringsAsFactors=FALSE, header=TRUE)

