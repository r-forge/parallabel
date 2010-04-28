############################################################################################################
#Function:	type4_p_parallel function
#Programer:	Unitsa Sangket
#Date:		2009
#Note:	for execute each task and merge output
############################################################################################################


"type4_p_parallel" <- function(fun,data_f,tasks,out_f,num_call_i,...){ 

load(data_f)

#Initialize MPI0+
library("Rmpi")

sep_tasks = tasks

# Notice we just say "give us all the slaves you've got."
mpi.spawn.Rslaves()

if (mpi.comm.size() < 2) {
    print("More slave processes are required.")
    mpi.quit()
    }

.Last <- function(){
    if (is.loaded("mpi_initialize")){
        if (mpi.comm.size(1) > 0){
            print("Please use mpi.close.Rslaves() to close slaves.")
            mpi.close.Rslaves()
        }
        print("Please use mpi.quit() to quit R")
        .Call("mpi_finalize")
    }
}


########## Function the slaves will call to perform a validation on the
# fold equal to their slave number.
# Assumes: fold,foldNumber
foldslave <- function(){
    # Note the use of the tag for sent messages: 
    #     1=ready_for_task, 2=done_task, 3=exiting 
    # Note the use of the tag for received messages: 
    #     1=task, 2=done_tasks 
    junk <- 0 
    done <- 0 

    while (done != 1) {
        # Signal being ready to receive a new task 
        mpi.send.Robj(junk,0,1) 

        # Receive a task 
        task <- mpi.recv.Robj(mpi.any.source(),mpi.any.tag()) 
        task_info <- mpi.get.sourcetag() 
        tag <- task_info[2] 

        if (tag == 1) {
			
			#************** 3.task for compute-node  ******************************
			# load GenABEL library
			library(GenABEL)
			
			# for test 
			#results <- list(foldNumber=task$foldNumber,output=task$foldNumber, subsquare=task$subsquare) 
			#return(results)
			########### end of for test

			subsquare = task$subsquare

			if (subsquare == 1){
				### test here is ok
				args_oth = args_oth12
				load(data_f)
				sset1 <- data@gtdata@snpnames[task$sset1_start:task$sset1_stop]
				data = data[,c(task$sset1_start:task$sset1_stop)]
				snpsubset = sset1
				foldNumber = task$foldNumber
				source(temp_fun12_type4_f)
				args_oth$data = data
				args_oth$snpsubset = snpsubset

				formals(temp_fun12_type4) = args_oth
				output=temp_fun12_type4()

				### test here is not ok
				# check progress
				progress_txt = paste("\n"," " ,date(), " ", "taskid=" , task$foldNumber, sep="")
				write(progress_txt, file = progress_type4_f,append = TRUE)
			

			}else if (subsquare == 2){
				args_oth = args_oth12
				load(data_f)
				sset2 <- data@gtdata@snpnames[task$sset2_start:task$sset2_stop]
				data = data[,c(task$sset2_start:task$sset2_stop)]
				snpsubset = sset2
				foldNumber = task$foldNumber
				source(temp_fun12_type4_f)
				args_oth$data = data
				args_oth$snpsubset = snpsubset
				args_oth$fun = fun

				formals(temp_fun12_type4) = args_oth
				output=temp_fun12_type4()

				### test here is not ok
				# check progress
				progress_txt = paste("\n"," " ,date(), " ", "taskid=" , task$foldNumber, sep="")
				write(progress_txt, file = progress_type4_f,append = TRUE)
			


			}else if (subsquare == 3){
				args_oth = args_oth34
				load(data_f)
				sset1 <- data@gtdata@snpnames[task$sset1_start:task$sset1_stop]
				sset3 <- data@gtdata@snpnames[task$sset3_start:task$sset3_stop]
				data = data[,c(task$sset1_start:task$sset3_stop)]
				snpsubset = sset1
				cross.snpsubset = sset3
				foldNumber = task$foldNumber
				source(temp_fun34_type4_f)
				args_oth$data = data
				args_oth$snpsubset = snpsubset
				args_oth$cross.snpsubset = cross.snpsubset
				args_oth$fun = fun

				formals(temp_fun34_type4) = args_oth
				output=temp_fun34_type4()

				### test here is not ok
				# check progress
				progress_txt = paste("\n"," " ,date(), " ", "taskid=" , task$foldNumber, sep="")
				write(progress_txt, file = progress_type4_f,append = TRUE)
			


			}else if (subsquare == 4){
				args_oth = args_oth34
				load(data_f)
				sset1 <- data@gtdata@snpnames[task$sset1_start:task$sset1_stop]
				sset4 <- data@gtdata@snpnames[task$sset4_start:task$sset4_stop]
				data = data[,c(task$sset1_start:task$sset4_stop)]
				snpsubset = sset1
				cross.snpsubset = sset4
				foldNumber = task$foldNumber
				source(temp_fun34_type4_f)
				args_oth$data = data
				args_oth$snpsubset = snpsubset
				args_oth$cross.snpsubset = cross.snpsubset
				args_oth$fun = fun
				
				formals(temp_fun34_type4) = args_oth
				output=temp_fun34_type4()

				### test here is not ok
				# check progress
				progress_txt = paste("\n"," " ,date(), " ", "taskid=" , task$foldNumber, sep="")
				write(progress_txt, file = progress_type4_f,append = TRUE)
			
				
			}else{
				message_error = "Error: subsquare error"
				output = message_error
			}
			
			#results <- list(subsquare=task$subsquare)
			results <- list(foldNumber=task$foldNumber,output=output, subsquare=task$subsquare) 
			rm(data)
			
			#***************** end task for compute-node ***************************

            mpi.send.Robj(results,0,2)
            }
        else if (tag == 2) {
            done <- 1
            }
        # We'll just ignore any unknown messages
    }

    mpi.send.Robj(junk,0,3)
}

########## We're in the parent. 
# receive tasks from variable

num_tasks = length(tasks)

# initial results
results <- vector('list')
for (i in 1:num_tasks) {
    results[[i]] <- list(output=i)
}

results_subsquare <- vector('list')
for (i in 1:num_tasks) {
    results_subsquare[[i]] <- list(subsquare=i)
}


# Now, send the data to the slaves

# Send the function to the slaves
mpi.bcast.Robj2slave(foldslave)

#******************************** 2. distribute task   ******************************
# Call the function in all the slaves to get them ready to
# undertake tasks

### prepairing args

### 1. for subsquare = 1 or 2
"temp12" <- function(data,...){
	rm(data)
	rm(snpsubset)

	# check this argument before remove

	if (missing(cross.snpsubset)) 
		rm(cross.snpsubset)

	if (missing(idsubset)) 
		rm(idsubset)

	args=ls()
	
	
	old_formals = formals(temp12)
	n_old_formals = names(old_formals)

	match_args = match(args, n_old_formals)
	temp = old_formals
	
	if (length(args) == 0) { #argument length zero
		temp = NULL
	}else{
		#update arguments of new_formals 
		for(i in 1:length(args)){
			if( !is.na(match_args[i])){
					temp_arg = get(args[i])
					if (!missing(temp_arg)){
						if (temp[[match_args[i]]] == temp_arg[1]) # default arg value
							temp[[match_args[i]]] = ""	
						else temp[[match_args[i]]] = temp_arg
					}
				
			}
		}

		#delete empty value argument
		i = 1 
		n_temp = names(temp)
		while ( i <= length(n_temp)){
			if (temp[i] == "")
				temp[i] <- NULL
			else 
				i = i + 1
		}
	} # end of else
	
	return(temp)  #return argument except data, snpsubset and argument the value same default
}

formals(temp12) = formals(fun)
args_oth12 = temp12(data,...)

#return(args_oth12)

### peparing call_fun

n_args_oth = names(args_oth12)
call_fun = paste("temp_fun12_type4 <- function(fun){", "\n", sep="")

call_fun = paste(call_fun, "output <- fun(data=data@gtdata[,snpsubset], snpsubset=snpsubset",sep = "")

if (length(n_args_oth) > 0){
	for(i in 1:length(n_args_oth)){
		call_fun = paste(call_fun,", ",n_args_oth[i], "=args_oth$", n_args_oth[i],sep="")
	}   
}

# insert ) and }
call_fun = paste(call_fun,")","\n", "return(output)", "\n", "}",sep="")
#write(call_fun, "temp_fun12_type4.R")

# generate subscript file
t_subscript <- 1:99999999
subscript = sample(t_subscript,1)
temp_fun12_type4_f = paste("temp_fun12_type4_",subscript,".R",sep = "")
write(call_fun, temp_fun12_type4_f)


### 2. for subsquare = 3 or 4
"temp34" <- function(data,...){
	rm(data)
	rm(snpsubset)
	rm(cross.snpsubset)

	# check this argument before remove
	if (missing(idsubset)) 
		rm(idsubset)


	args=ls()

	old_formals = formals(temp34)
	n_old_formals = names(old_formals)

	match_args = match(args, n_old_formals)
	temp = old_formals
	
	if (length(args) == 0) { # argument length zero
		temp = NULL
	}else{
		#update new_formals
		for(i in 1:length(args)){
			if( !is.na(match_args[i])){
					temp_arg = get(args[i])
					if (!missing(temp_arg)){
						if (temp[[match_args[i]]] == temp_arg[1]) # default arg value
							temp[[match_args[i]]] = ""	
						else temp[[match_args[i]]] = temp_arg
					}
				
			}
		}

		#delete empty value argument
		i = 1 
		n_temp = names(temp)
		while ( i <= length(n_temp)){
			if (temp[i] == "")
				temp[i] <- NULL
			else 
				i = i + 1
		}
	} # end of else
			
	return(temp)  # return all arguments except data, snpsubset and the argument which have default value
}

formals(temp34) = formals(fun)
args_oth34 = temp34(data,...)


### peparing call_fun

n_args_oth = names(args_oth34)
call_fun = paste("temp_fun34_type4 <- function(fun){", "\n", sep="")

call_fun = paste(call_fun, "output <- fun(data=data@gtdata[,c(snpsubset,cross.snpsubset)], snpsubset=snpsubset,cross.snpsubset=cross.snpsubset",sep = "")

if (length(n_args_oth) > 0){
	for(i in 1:length(n_args_oth)){
		call_fun = paste(call_fun,", ",n_args_oth[i], "=args_oth$", n_args_oth[i],sep="")
	}   
}

# insert ) and }
call_fun = paste(call_fun,")","\n", "return(output)", "\n", "}",sep="")
#write(call_fun, "temp_fun34_type4.R")

temp_fun34_type4_f = paste("temp_fun34_type4_",subscript,".R",sep = "")
write(call_fun, temp_fun34_type4_f)

# check progress
progress_type4_f = paste(output_f, "_progress_type4_",num_call_i,".txt",sep = "")

### send argument
#mpi.bcast.Robj2slave(data)
mpi.bcast.Robj2slave(temp_fun12_type4_f)
mpi.bcast.Robj2slave(temp_fun34_type4_f)
mpi.bcast.Robj2slave(data_f)
mpi.bcast.Robj2slave(args_oth12)
mpi.bcast.Robj2slave(args_oth34)
mpi.bcast.Robj2slave(fun)
mpi.bcast.Robj2slave(progress_type4_f)
mpi.bcast.cmd(foldslave())

progress_txt = paste("\n"," send argument success ", sep="")
write(progress_txt, file = progress_type4_f,append = TRUE)

#********************************* finish task distribution ****************************

junk <- 0 
closed_slaves <- 0 
n_slaves <- mpi.comm.size()-1 


while (closed_slaves < n_slaves) { 
    # Receive a message from a slave 
    message <- mpi.recv.Robj(mpi.any.source(),mpi.any.tag()) 
    message_info <- mpi.get.sourcetag() 
    slave_id <- message_info[1] 
    tag <- message_info[2] 

    if (tag == 1) { 
        # slave is ready for a task. Give it the next task, or tell it tasks 
        # are done if there are none. 
        if (length(tasks) > 0) { 
            # Send a task, and then remove it from the task list 
            mpi.send.Robj(tasks[[1]], slave_id, 1); 
            tasks[[1]] <- NULL 
            } 
        else{ 
            mpi.send.Robj(junk, slave_id, 2) 
            } 
    } 
    else if (tag == 2) {

		#************************ 4.store result *************************
		# The message contains results. Do something with the results. 
		# Store them in the data structure
		results[[message$foldNumber]] = message$output
		results_subsquare[[message$foldNumber]] = message$subsquare

		# check progress
		#progress_txt = paste("\n"," " ,date(), " ", message$foldNumber, sep="")
		#write(progress_txt, file = progress_type4_f,append = TRUE)
		#******************************************************************

    } 
    else if (tag == 3) { 
        # A slave has closed down. 
        closed_slaves <- closed_slaves + 1 
    } 

} 



#*************************** 5.combine result ************************
# combine order by snpnames because may be slave2 finish before slave1 

#return(sep_tasks[[3]]$sset3_start)

#stop("pause")

#return(results)

##### create output matrix
#return(results)

if (length(data@gtdata@snpnames) < 10001){
#if (length(data@gtdata@snpnames) < 800){
	out <- matrix(ncol=data@gtdata@nsnps,nrow=data@gtdata@nsnps)
	colnames(out) <- data@gtdata@snpnames
	rownames(out) <- data@gtdata@snpnames

	for( i in 1:length(results)){
		if (results_subsquare[[i]] == 1){
			sset1 <- data@gtdata@snpnames[sep_tasks[[i]]$sset1_start:sep_tasks[[i]]$sset1_stop]
			out[sset1,sset1] <- results[[i]]
		}
		else if (results_subsquare[[i]] == 2){
			sset2 <- data@gtdata@snpnames[sep_tasks[[i]]$sset2_start:sep_tasks[[i]]$sset2_stop]
			out[sset2,sset2] <- results[[i]]
		}
		else if (results_subsquare[[i]] == 3){
			sset1 <- data@gtdata@snpnames[sep_tasks[[i]]$sset1_start:sep_tasks[[i]]$sset1_stop]
			sset3 <- data@gtdata@snpnames[sep_tasks[[i]]$sset3_start:sep_tasks[[i]]$sset3_stop]
			out[sset3,sset1] <- results[[i]][[1]]		#results[[i]]$num
			out[sset1,sset3] <- results[[i]][[2]]		#results[[i]]$r2
		}
		else if (results_subsquare[[i]] == 4){
			sset1 <- data@gtdata@snpnames[sep_tasks[[i]]$sset1_start:sep_tasks[[i]]$sset1_stop]
			sset4 <- data@gtdata@snpnames[sep_tasks[[i]]$sset4_start:sep_tasks[[i]]$sset4_stop]
			out[sset4,sset1] <- results[[i]][[1]]
			out[sset1,sset4] <- results[[i]][[2]]
		}
		else
			stop("Error: result from subsquare")

	} # end of loop for

	results = out 

}

sub_out_type4_f = paste(output_f,"_",num_call_i,".Rdata",sep = "")
save(results, file=sub_out_type4_f)

mpi.close.Rslaves()

# file.remove(data_f)
file.remove(temp_fun12_type4_f)
file.remove(temp_fun34_type4_f)

rm(list=ls())

sub_out_type4_f = "ok"

return(sub_out_type4_f)

} # End of function

