
############################################################################################################
#Function:	parallel type4 function
#Programer:	Unitsa Sangket
#Date:		2009
#Note:	a example of type4.p function is r2fast
#	    npro = number of processors on compute node
#       num_tasks = number of process
############################################################################################################


# separate task
# call type4_p_parallel.R
# send argument tasks, results, results_subsquare 

"type4.p" <- function(npro,fun,data="no",data_f="no",output_f,...){ 

#*********************************** 1.separate task   ******************************
# load GenABEL library
library(GenABEL)

if (missing(npro)) 
    stop("Missing number of processors")

if (missing(fun)) 
    stop("Missing function name")

if (missing(data) && missing(data_f)) 
    stop("Missing data")

if (missing(output_f)) 
    stop("Missing output file name")

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

##### check number of snps
number_of_snps = data@gtdata@nsnps
if (number_of_snps < 11)
    stop("The data is too small.")

##### check arguments
#sset1 <- data@gtdata@snpnames[1:5]
#sset2 <- data@gtdata@snpnames[6:10]
#a = fun(data@gtdata[,c(sset1,sset2)],snpsubset=sset1,cross.snpsubset=sset2,...)

##### separate data

# create sep_tasks
sep_tasks <- vector('list')
sep_tasks_square <- vector('list')

# first square
len_snp = length(data@gtdata@snpnames) 
sset1_start = 1
sset1_stop = floor(len_snp/2)
sset2_start = sset1_stop + 1
sset2_stop = len_snp
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

len_square_1 = sep_tasks[[1]]$sset1_stop - sep_tasks[[1]]$sset1_start + 1 

while (num_tasks < npro || len_square_1 > 7000){
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
	len_square_1 = sep_tasks[[1]]$sset1_stop - sep_tasks[[1]]$sset1_start + 1 

} #end of while loop

tasks_all = sep_tasks

#return(tasks)



# call function type4_p_parallel (can compute 32 subset for 1 time)

library(ParallABEL)
#source('type4_p_parallel.R')

if (length(tasks_all) < 8 || length(data@gtdata@snpnames) < 10000 ){
	tasks = tasks_all
	rm(data)
	results_ok = type4_p_parallel(fun=fun,data_f=data_f,tasks=tasks,out_f=out_f,num_call_i=1,...)
}else{
	# must compute more than one time
	num_call = round(length(tasks_all)/8)
	
	# first time
	tasks = tasks_all[1:8]
	rm(data)
	results_ok = type4_p_parallel(fun=fun,data_f=data_f,tasks=tasks,out_f=out_f,num_call_i=1,...)
	
	num_call_i = 2

	############### last check for 256 square (not complete)
	while(num_call_i < num_call){
		tasks = tasks_all[((num_call_i-1)*8+1):(num_call_i*8)]
		rm(data)
		results_ok = type4_p_parallel(fun=fun,data_f=data_f,tasks=tasks,out_f=out_f,num_call_i=num_call_i,...)
		num_call_i = num_call_i + 1
	}
	##############

	# last time
	tasks = tasks_all[((num_call_i-1)*8+1):length(tasks_all)]
	results_ok = type4_p_parallel(fun=fun,data_f=data_f,tasks=tasks,out_f=out_f,num_call_i=num_call_i,...)
}

#********************************** finish task separation *****************************

#input is in out_f_xxx.Rdata

if (data_f_n==1){ # if data is loaded from a file, then
file.remove(data_f)
}


rm(list=ls())
results_ok = 1


return(results_ok)

} # end of function

