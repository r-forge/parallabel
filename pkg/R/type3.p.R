############################################################################################################
#Function:	parallel type3 function
#Programer:	Unitsa Sangket
#Date:		2009
#Note:	a example of type3.p function is ibs
#	    npro = number of processors
#       num_tasks = number of process
############################################################################################################

"type3.p" <- function(npro,fun,data="no",data_f="no",...){ 

#Initialize MPI
library("Rmpi")

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
			library(GenABEL)
			# load GenABEL library

			#results <- list(foldNumber=task$foldNumber,output=task$subsquare + 1)
			
			subsquare = task$subsquare

			if (subsquare == 1){
				args_oth = args_oth12
				load(data_f)
				sset1 <- data@gtdata@idnames[task$sset1_start:task$sset1_stop]
				data = data[c(task$sset1_start:task$sset1_stop),]
				idsubset = sset1
				foldNumber = task$foldNumber
				source(temp_fun12_type3_f)
				args_oth$data = data
				args_oth$idsubset = idsubset
			
				formals(temp_fun12_type3) = args_oth
				output=temp_fun12_type3()

				
			}else if (subsquare == 2){
				args_oth = args_oth12
				load(data_f)
				sset2 <- data@gtdata@idnames[task$sset2_start:task$sset2_stop]
				data = data[c(task$sset2_start:task$sset2_stop),]
				idsubset = sset2
				foldNumber = task$foldNumber
				source(temp_fun12_type3_f)
				args_oth$data = data
				args_oth$idsubset = idsubset
				args_oth$fun = fun
				
				formals(temp_fun12_type3) = args_oth
				output=temp_fun12_type3()


			}else if (subsquare == 3){
				args_oth = args_oth34
				load(data_f)
				sset1 <- data@gtdata@idnames[task$sset1_start:task$sset1_stop]
				sset3 <- data@gtdata@idnames[task$sset3_start:task$sset3_stop]
				data = data[c(task$sset1_start:task$sset3_stop),]
				idsubset = sset1
				cross.idsubset = sset3
				foldNumber = task$foldNumber
				source(temp_fun34_type3_f)
				args_oth$data = data
				args_oth$idsubset = idsubset
				args_oth$cross.idsubset = cross.idsubset
				args_oth$fun = fun

				formals(temp_fun34_type3) = args_oth
				output=temp_fun34_type3()


			}else if (subsquare == 4){
				args_oth = args_oth34
				load(data_f)
				sset1 <- data@gtdata@idnames[task$sset1_start:task$sset1_stop]
				sset4 <- data@gtdata@idnames[task$sset4_start:task$sset4_stop]
				data = data[c(task$sset1_start:task$sset4_stop),]
				idsubset = sset1
				cross.idsubset = sset4
				foldNumber = task$foldNumber
				source(temp_fun34_type3_f)
				args_oth$data = data
				args_oth$idsubset = idsubset
				args_oth$cross.idsubset = cross.idsubset
				args_oth$fun = fun

				formals(temp_fun34_type3) = args_oth
				output=temp_fun34_type3()

			}else{
				message_error = "Error: subsquare error"
				output = message_error
			}
			
			#results <- list(subsquare=task$subsquare)
			results <- list(foldNumber=task$foldNumber,output=output, subsquare=task$subsquare) 
			

			#***************** end task for compute-node ***************************
			rm(data)
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

#*********************************** 1.separate task   ******************************
library(GenABEL)
# load GenABEL library

if (missing(npro)) 
    stop("Missing number of processors")

if (missing(fun)) 
    stop("Missing function name")

if (missing(data) && missing(data_f)) 
    stop("Missing data")

# generate subscript file
t_subscript <- 1:99999999
subscript = sample(t_subscript,1)

if(data_f == "no"){ # there are data file
	data_f = paste("data_",subscript,".Rdata",sep = "")
	save(data,file=data_f)
	data_f_n = 1
}
else{ 
	data_f_n = 0
	load(data_f)
}

##### check number of ids
number_of_ids = data@gtdata@nids
if (number_of_ids < 11)
    stop("The data is too small.")

##### check arguments
sset1 <- data@gtdata@idnames[1:5]
sset2 <- data@gtdata@idnames[6:10]
a = fun(data@gtdata,idsubset=sset1,cross.idsubset=sset2,...)

##### separate data

# create sep_tasks
sep_tasks <- vector('list')
sep_tasks_square <- vector('list')

# first square
len_id = length(data@gtdata@idnames) 
sset1_start = 1
sset1_stop = floor(len_id/2)
sset2_start = sset1_stop + 1
sset2_stop = len_id
sset3_start = sset1_stop + 1
sset3_stop = sset3_start + floor(((sset2_stop-sset2_start+1)/2)) - 1
sset4_start = sset3_stop + 1
sset4_stop = sset2_stop

square = 1
#subsquare = 1 => compute sset1
#subsquare = 2 => compute sset2
#subsquare = 3 => compute cross first part
#subsquare = 3 => compute cross second part
sep_tasks[[1]] <- list(foldNumber=1,subsquare=1,sset1_start=sset1_start,sset1_stop=sset1_stop)
sep_tasks[[2]] <- list(foldNumber=2,subsquare=2,sset2_start= sset2_start,sset2_stop=sset2_stop)
sep_tasks[[3]] <- list(foldNumber=3,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset3_start,sset3_stop=sset3_stop)
sep_tasks[[4]] <- list(foldNumber=4,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset4_start,sset4_stop=sset4_stop)

sep_tasks_square[[1]] = sep_tasks
num_tasks = 4

while (num_tasks < npro){
	sep_tasks_old = sep_tasks
	#i is index_sep_tasks_old
	#j is index_sep_tasks
	j = 0 
	square =  square + 1
	for(i in 1:length(sep_tasks_old)){
		subsquare=sep_tasks_old[[i]]$subsquare 
		if (subsquare == 1){
			len_square = sep_tasks_old[[i]]$sset1_stop - sep_tasks_old[[i]]$sset1_start + 1 
			if (len_square == 10)
				stop("Error: task too small")
			sset1_start = sep_tasks_old[[i]]$sset1_start
			sset1_stop = sset1_start + floor(len_square/2) - 1
			sset2_start = sset1_stop + 1
			sset2_stop = sep_tasks_old[[i]]$sset1_stop
			sset3_start = sset1_stop + 1
			sset3_stop = sset3_start + floor(((sset2_stop-sset2_start+1)/2))-1 
			sset4_start = sset3_stop + 1
			sset4_stop = sset2_stop

			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=1,sset1_start=sset1_start,sset1_stop=sset1_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=2,sset2_start= sset2_start,sset2_stop=sset2_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset3_start,sset3_stop=sset3_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset4_start,sset4_stop=sset4_stop)

		}else if (subsquare == 2){ 
			len_square = sep_tasks_old[[i]]$sset2_stop - sep_tasks_old[[i]]$sset2_start + 1 
			if (len_square == 10)
				stop("Error: task too small")
			sset1_start = sep_tasks_old[[i]]$sset2_start
			sset1_stop = sset1_start + floor(len_square/2) - 1
			sset2_start = sset1_stop + 1
			sset2_stop = sep_tasks_old[[i]]$sset2_stop
			sset3_start = sset1_stop + 1
			sset3_stop = sset3_start + floor(((sset2_stop-sset2_start+1)/2))-1 
			sset4_start = sset3_stop + 1
			sset4_stop = sset2_stop
			
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=1,sset1_start=sset1_start,sset1_stop=sset1_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=2,sset2_start= sset2_start,sset2_stop=sset2_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset3_start,sset3_stop=sset3_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset4_start,sset4_stop=sset4_stop)
		

		}else if (subsquare == 3){
			len_square = sep_tasks_old[[i]]$sset3_stop-sep_tasks_old[[i]]$sset3_start+1
			if (len_square == 10)
				stop("Error: task too small")
			sset1_start = sep_tasks_old[[i]]$sset1_start
			sset1_stop = sep_tasks_old[[i]]$sset1_stop
			len_sub_square = floor(len_square/4)
			sset31_start = sep_tasks_old[[i]]$sset3_start
			sset31_stop = sset31_start + len_sub_square - 1 
			sset32_start = sset31_stop + 1
			sset32_stop = sset32_start + len_sub_square - 1 
			sset33_start = sset32_stop + 1
			sset33_stop = sset33_start + len_sub_square - 1 
			sset34_start = sset33_stop + 1
			sset34_stop = sep_tasks_old[[i]]$sset3_stop 
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset31_start,sset3_stop=sset31_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset32_start,sset3_stop=sset32_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset33_start,sset3_stop=sset33_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=3,sset1_start=sset1_start,sset1_stop=sset1_stop,sset3_start= sset34_start,sset3_stop=sset34_stop)
		
		}else if (subsquare == 4){
			if (len_square == 10)
				stop("Error: task too small")
			len_square = sep_tasks_old[[i]]$sset4_stop-sep_tasks_old[[i]]$sset4_start+1
			sset1_start = sep_tasks_old[[i]]$sset1_start
			sset1_stop = sep_tasks_old[[i]]$sset1_stop
			len_sub_square = floor(len_square/4)
			sset41_start = sep_tasks_old[[i]]$sset4_start
			sset41_stop = sset41_start + len_sub_square - 1 
			sset42_start = sset41_stop + 1
			sset42_stop = sset42_start + len_sub_square - 1 
			sset43_start = sset42_stop + 1
			sset43_stop = sset43_start + len_sub_square - 1 
			sset44_start = sset43_stop + 1
			sset44_stop = sep_tasks_old[[i]]$sset4_stop 
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset41_start,sset4_stop=sset41_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset42_start,sset4_stop=sset42_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset43_start,sset4_stop=sset43_stop)
			j = j + 1
			sep_tasks[[j]] <- list(foldNumber=j,parent_num = i,subsquare=4,sset1_start=sset1_start,sset1_stop=sset1_stop,sset4_start= sset44_start,sset4_stop=sset44_stop)
		
		}else{
			stop("Error: subsquare error")
		}
		
	} #end of for loop

	sep_tasks_square[[square]] = sep_tasks
	num_tasks = length(sep_tasks)

} #end of while loop

tasks = sep_tasks

# initial results
results <- vector('list')
for (i in 1:num_tasks) {
    results[[i]] <- list(output=i)
}

results_subsquare <- vector('list')
for (i in 1:num_tasks) {
    results_subsquare[[i]] <- list(subsquare=i)
}

#********************************** finish task separation *****************************

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
	rm(idsubset)
	

	# check this argument before remove
	if (missing(snpsubset)) 
		rm(snpsubset)

	if (missing(cross.idsubset)) 
		rm(cross.idsubset)

	if (missing(snpfreq)) 
		rm(snpfreq)

	args=ls()

	old_formals = formals(temp12)
	n_old_formals = names(old_formals)

	match_args = match(args, n_old_formals)
	temp = old_formals
	
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
			
	return(temp)  #return argument except data, idsubset and argument the value same default
}

formals(temp12) = formals(fun)
args_oth12 = temp12(data,...)

#return(args_oth12)

### peparing call_fun

n_args_oth = names(args_oth12)
call_fun = paste("temp_fun12_type3 <- function(fun){", "\n", sep="")

call_fun = paste(call_fun, "output <- fun(data=data, idsubset=idsubset",sep = "")

if (length(n_args_oth) > 0){
	for(i in 1:length(n_args_oth)){
		call_fun = paste(call_fun,", ",n_args_oth[i], "=args_oth$", n_args_oth[i],sep="")
	}   
}

# insert ) and }
call_fun = paste(call_fun,")","\n", "return(output)", "\n", "}",sep="")
# write(call_fun, "temp_fun12_type3.R")

temp_fun12_type3_f = paste("temp_fun12_type3_",subscript,".R",sep = "")
write(call_fun, temp_fun12_type3_f)


### 2. for subsquare = 3 or 4
"temp34" <- function(data,...){
	rm(data)
	rm(idsubset)
	rm(cross.idsubset)
	

	# check this argument before remove
	if (missing(snpsubset)) 
		rm(snpsubset)

	if (missing(snpfreq)) 
		rm(snpfreq)

	args=ls()

	old_formals = formals(temp34)
	n_old_formals = names(old_formals)

	match_args = match(args, n_old_formals)
	temp = old_formals
	
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
			
	return(temp)  # return all arguments except data, snpsubset and the argument which have default value
}

formals(temp34) = formals(fun)
args_oth34 = temp34(data,...)


### peparing call_fun

n_args_oth = names(args_oth34)
call_fun = paste("temp_fun34_type3 <- function(fun){", "\n", sep="")

call_fun = paste(call_fun, "output <- fun(data=data, idsubset=idsubset,cross.idsubset=cross.idsubset",sep = "")

if (length(n_args_oth) > 0){
	for(i in 1:length(n_args_oth)){
		call_fun = paste(call_fun,", ",n_args_oth[i], "=args_oth$", n_args_oth[i],sep="")
	}   
}

# insert ) and }
call_fun = paste(call_fun,")","\n", "return(output)", "\n", "}",sep="")


temp_fun34_type3_f = paste("temp_fun34_type3_",subscript,".R",sep = "")
write(call_fun, temp_fun34_type3_f)


data_f = paste("data_",subscript,".Rdata",sep = "")
save(data,file=data_f)

#write(call_fun, "temp_fun34_type3.R")

### send argument
mpi.bcast.Robj2slave(temp_fun12_type3_f)
mpi.bcast.Robj2slave(temp_fun34_type3_f)
mpi.bcast.Robj2slave(data_f)
mpi.bcast.Robj2slave(args_oth12)
mpi.bcast.Robj2slave(args_oth34)
mpi.bcast.Robj2slave(fun)
mpi.bcast.cmd(foldslave())

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
		#******************************************************************

    } 
    else if (tag == 3) { 
        # A slave has closed down. 
        closed_slaves <- closed_slaves + 1 
    } 

} 

mpi.close.Rslaves()

#*************************** 5.combine result ************************
# combine order by snpnames because may be slave2 finish before slave1 

#return(sep_tasks[[3]]$sset3_start)

#stop("pause")

#return(results)

out <- matrix(ncol=data@gtdata@nids,nrow=data@gtdata@nids)
colnames(out) <- data@gtdata@idnames
rownames(out) <- data@gtdata@idnames

for( i in 1:length(results)){
	if (results_subsquare[[i]] == 1){
		sset1 <- data@gtdata@idnames[sep_tasks[[i]]$sset1_start:sep_tasks[[i]]$sset1_stop]
		out[sset1,sset1] <- results[[i]]
	}
	else if (results_subsquare[[i]] == 2){
		sset2 <- data@gtdata@idnames[sep_tasks[[i]]$sset2_start:sep_tasks[[i]]$sset2_stop]
		out[sset2,sset2] <- results[[i]]
	}
	else if (results_subsquare[[i]] == 3){
		sset1 <- data@gtdata@idnames[sep_tasks[[i]]$sset1_start:sep_tasks[[i]]$sset1_stop]
		sset3 <- data@gtdata@idnames[sep_tasks[[i]]$sset3_start:sep_tasks[[i]]$sset3_stop]
		out[sset3,sset1] <- results[[i]][[1]]	#results[[i]]$ibs
		out[sset1,sset3] <- results[[i]][[2]]	#results[[i]]$num
	}
	else if (results_subsquare[[i]] == 4){
		sset1 <- data@gtdata@idnames[sep_tasks[[i]]$sset1_start:sep_tasks[[i]]$sset1_stop]
		sset4 <- data@gtdata@idnames[sep_tasks[[i]]$sset4_start:sep_tasks[[i]]$sset4_stop]
		out[sset4,sset1] <- results[[i]][[1]]
		out[sset1,sset4] <- results[[i]][[2]]
	}
	else
		stop("Error: result from subsquare")

} # end of loop for

results = out 

if (data_f_n==1){ # if data is loaded from a file, then
file.remove(data_f)
}

file.remove(temp_fun12_type3_f)
file.remove(temp_fun34_type3_f)

return(results)

} # End of function


