############################################################################################################
#Function:	parallel type1 function
#Programer:	Unitsa Sangket
#Date:		2009
#Objective:	
#Note:	a example of type1.p function is mlreg.p
############################################################################################################

"type1.p" <- function(npro,fun,data,data_f="no",...){ 

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
			# load GenABEL library
			library(GenABEL)

			snpsubset  = task$snpsubset
			foldNumber = task$foldNumber
			source(temp_fun_type1_f)
			load(data_f)
			
			## edit fro test eigth_core
			start = task$start
			stop = task$stop
			data <- data[,start:stop]
			###

			args_oth$data = data
			args_oth$snpsubset = snpsubset
			args_oth$fun = fun
			formals(temp_fun) = args_oth
			output=temp_fun()

			results <- list(foldNumber=foldNumber,output=output) 
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

#*********************************** 1.separate task   ******************************
# load GenABEL library
library(GenABEL)

if (missing(npro)) 
    stop("Missing number of processors")

if (missing(fun)) 
    stop("Missing function name")

if (missing(data) && missing(data_f)) 
    stop("Missing data")

# generate subscript file
t_subscript <- 1:99999999
subscript = sample(t_subscript,1)

if(data_f == "no"){ # there are no data file
	data_f = paste("data_",subscript,".Rdata",sep = "")
	save(data,file=data_f)
	data_f_n = 1
}
else{ 
	data_f_n = 0
	load(data_f)
}

##### check number of snps
number_of_snps = data@gtdata@nsnps
if (number_of_snps < 11)
    stop("The data is too small.")

##### check arguments
snpsubset <- data@gtdata@snpnames[1:10]
a = fun(data=data,snpsubset=snpsubset,...)

##### separate data
nsnps = length(data@gtdata@snpnames) 
nsnps_p = floor(nsnps/npro)
pointer = 0

#create data@gtdata@snpnames = data@gtdata@snpnames[start:stop] 
#Create task list
tasks <- vector('list')
for (i in 1:(npro-1)) {
    tasks[[i]] <- list(foldNumber=i,snpsubset=data@gtdata@snpnames[(pointer + 1):(pointer + nsnps_p)], start = (pointer + 1), stop = (pointer + nsnps_p))
    pointer = pointer + nsnps_p
}
#last process
i = i + 1
tasks[[i]] <- list(foldNumber=i, snpsubset=data@gtdata@snpnames[(pointer + 1):(nsnps)], start = (pointer + 1), stop = nsnps) 

# initial results
results <- vector('list')
for (i in 1:npro) {
    results[[i]] <- list(output=i)
}

#********************************** finish task separation  *****************************

# Now, send the data to the slaves

# Send the function to the slaves
mpi.bcast.Robj2slave(foldslave)

#******************************** 2. distribute task   ******************************
# Call the function in all the slaves to get them ready to
# undertake tasks

### prepairing args

"temp" <- function(data,...){
	# argument must add later
	rm(data)
	rm(snpsubset)
	
	# check this argument before remove
	if (missing(idsubset)) 
		rm(idsubset)

	args=ls()

	old_formals = formals(temp)
	n_old_formals = names(old_formals)

	match_args = match(args, n_old_formals)
	temp = old_formals
	
	#update arguments of new_formals
	for(i in 1:length(args)){
		if( !is.na(match_args[i])){
			if (temp[[match_args[i]]] == get(args[i])) # default arg value
				temp[[match_args[i]]] = ""	
			else temp[[match_args[i]]] = get(args[i])
			
		}
	}

	# delete empty value arguments
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

formals(temp) = formals(fun)

args_oth = temp(data=data,...)

### peparing call_fun

n_args_oth = names(args_oth)
call_fun = paste("temp_fun <- function(fun){", "\n", sep="")

call_fun = paste(call_fun, "output <- fun(data=data, snpsubset=snpsubset",sep = "")

if (length(n_args_oth) > 0 ){
	for(i in 1:length(n_args_oth)){
		call_fun = paste(call_fun,", ",n_args_oth[i], "=args_oth$", n_args_oth[i],sep="")
	}   
}

# insert ) and }
call_fun = paste(call_fun,")","\n", "return(output)", "\n", "}",sep="")

temp_fun_type1_f = paste("temp_fun_type1_",subscript,".R",sep = "")

write(call_fun, temp_fun_type1_f)

### send argument
mpi.bcast.Robj2slave(temp_fun_type1_f)
mpi.bcast.Robj2slave(data_f)
mpi.bcast.Robj2slave(args_oth)
mpi.bcast.Robj2slave(fun)
mpi.bcast.cmd(foldslave())

rm(data)

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

#return(results)

#stop("pause")

results_list = results
results = results_list[[1]]

##check structure of result
# if data.frame use rbind
if (is.data.frame(results)) { 
	for (i in 2:npro) {
		results = rbind(results,results_list[[i]])
	}

}else if (is.list(results)){

	# create flag_do array
	# flag = 1 -> will combine, flag = 0 -> not combine
	n_results = names(results_list[[1]])
	flag_do = n_results 
	for(i in 1:length(flag_do)){
		if ( (length(results_list[[1]][[n_results[i]]]) == length(results_list[[1]][["snpnames"]]) ) && (n_results[i] != "idnames") ) 
			flag_do[i] = 1
		else flag_do[i] = 0
	}

	# combine results
	results = results_list[[1]]
	for (i in 2:npro) {
		for (j in 1:length(n_results)){
			if (flag_do[j] == 1)
				results[[n_results[j]]] = c(results[[n_results[j]]], results_list[[i]][[n_results[j]]])
		} 
	}
	
}else {
	message_error = paste("Error: structure of result from ", fun, " doesn't list or data.frame", sep = "")
	stop(message_error)
}

# remove data_f, temp_fun_type1_f 

if (data_f_n==1){ # if data is loaded from a file, then
file.remove(data_f)
}

file.remove(temp_fun_type1_f)

return(results)

} # End of function



