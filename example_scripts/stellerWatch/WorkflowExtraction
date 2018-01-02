#Processes a CSV file downloaded from 'Steller Watch'. Year must be in the format Y-M-D. top100 only reads the first 100 rows from the CSV for testing.

#The columns must include 'created_at', 'workflow_version', 'annotations' (a JSON value with a 'task', 'task_label', and 'value'), and 'subject_ids' (a JSON value structures like '{subject_id:{retired:...,Filename:...}}').

#result is a flat structure of subject_ids, photo_name, frame_num, followed by counts for each workflow-task-answer.
#The count columns in result are labelled workflow#_taskname_answer.
#Null answers are not included in the final result.

#Example use
result <- ProcessCSV(
    fileName = "//Nmfs/akc-nmml/Alaska/Data/Steller Sea Lion/Remote Cameras/Zooniverse/Dataexports/2_Full/steller-watch-classifications_USETHISONE.csv"
    , minDate = "2017-5-14"
    , maxDate = "2017-7-12" 
    , top100 = FALSE
  )
  
ProcessCSV <- function(fileName, minDate, maxDate, top100) {
  if (missing(top100) || !top100) {
    df <- read.csv(file=fileName)
  } else {
    df <- read.csv(file=fileName, nrows=100)
  }
  if (!require("jsonlite")) {
    install.packages("jsonlite")
    require("jsonlite")
  }
  if (!require("dplyr")) {
    install.packages("dplyr")
    require("dplyr")
  }
  #You can use reshape instead, but have to change melt() parameters.
  if (!require("reshape2")) {
    install.packages("reshape2")
    require("reshape2")
  }
  #subset by created_at date range
  if (!missing(minDate) && !missing(maxDate)) {
    df <- subset(df, as.Date(created_at) >= as.Date(minDate) & as.Date(created_at) <= as.Date(maxDate))
  } else if (!missing(minDate)) {
    df <- subset(df, as.Date(created_at) >= as.Date(minDate))
  } else if (!missing(maxDate)) {
    df <- subset(df, as.Date(created_at) <= as.Date(maxDate))
  }
  #Collapse df with frequencies
  df <- df %>% group_by(annotations, workflow_id, workflow_name, workflow_version, subject_ids, subject_data) %>% summarise(count = n())
  #split df into a data.frame and work on them separately for each workflow_id
  df <- split(df, f = df$workflow_id)
  #initialize a data.frame to merge all results with.
  result <- data.frame(subject_ids = character(), photo_name = character(), frame_num = character())
  #Iterate through the workflows
  for (a in 1:length(df)) {
    workflow <- df[[a]]
    #transpose tasks so that task number is column name, and concatenate values from a single task.
    #Had trouble doing this with a mutate() or apply() because of dynamic column names.
    for(i in 1:nrow(workflow)) {
      ann <- fromJSON(as.character(workflow[[i,'annotations']]))
      sub <- fromJSON(as.character(workflow[[i,'subject_data']]))[[1]]
      #NA to replace NULLs otherwise you get a replacement has length zero error
      photoname <- c(sub[['#ImageName']], sub[['#image_name']], sub[['Filename']], sub[['#Filename']], NA)
      imagenum <- c(sub[['#Frame']], NA)
      workflow[i,'photo_name'] <- photoname[which(!is.null(photoname))[1]]
      workflow[i,'frame_num'] <- imagenum[which(!is.null(imagenum))[1]]
      for(j in 1:nrow(ann)) {
        task <- ann[j,'task']
        value <- paste(unlist(ann[j,'value']), collapse = ', ')
        if (value == '') {
          value <- NA
        }
        workflow[i,task] <- value
      }
    }
    #drop annotations and subject_data from output
    workflow <- workflow[, !(names(workflow) %in% c("annotations", "subject_data"))]
    #Have to convert to data.frame because dplyr outputs it as a tibble which cannot be passed into melt()
    workflow <- data.frame(workflow, stringsAsFactors = TRUE)
    #unpivot
    workflow <- melt(workflow, id = c("subject_ids", "photo_name", "frame_num", "workflow_id", "workflow_name", "workflow_version", "count"), na.rm = FALSE)
    #Re-count
    workflow <- workflow %>% group_by(subject_ids, photo_name, frame_num, workflow_id, workflow_name, workflow_version, variable, value) %>% summarise(count = sum(count))
    #rename columns because cast() cares about "variable" and "value", and manually setting those as parameters does not work.
    data.table::setnames(workflow, old = c("variable", "value", "count"), new = c("task", "variable", "value"))
    #split by task
    workflow <- split(workflow, f = workflow$task)
    #Iterate through the tasks
    for (k in 1:length(workflow)) {
      #pivot tables by $variable (task answers), with summed counts
      task <- reshape2::dcast(workflow[[k]], subject_ids + photo_name + frame_num ~ variable, sum)
      task <- task[, names(task) != "NA"]
      workflow[[k]] <- task
      #merge with the final results
      colnames(task) <- c(colnames(task)[1:3], paste(names(df[a]), names(workflow[k]), colnames(task)[-(1:3)], sep="_"))
      result <- merge(result, task, by=1:3, all=TRUE)
    }
    #set to df[[a]]
    df[[a]] <- workflow
  }
  return(result)
  #return(df)
  #can also return df for a result structured like this:
  #   named list :
  #   [
  #     workflow_id 1 :
  #       [
  #         task T0 : data.frame,
  #         task T1 : data.frame,
  #         ...
  #       ],
  #     workflow_id 2 :
  #       [
  #         task T0: data.frame,
  #         task T1 : data.frame,
  #         ...
  #       ],
  #     ...
  #   ]
  #colnames(data.frame) = subject_ids, photo_name, frame_num, count of task answer 1, count of task answer 2, ...
}
