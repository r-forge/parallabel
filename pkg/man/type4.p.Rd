\name{type4.p}
\alias{type4.p}
\title{Parallel for the pairwise statistics derived from each pair of SNP}
\description{
	Parallel for pairwise statistics derived from each pair of SNP, e.g. linkage disequilibrium characterisation  
}
\usage{
type4.p(npro,fun,data,data_f,output_f,...)
}
\arguments{
  \item{npro}{number of processors on compute nodes 
		}
  \item{fun}{function name will be processed such as r2fast 
		}
  \item{data}{object of snp.data-class}
  \item{data_f}{file name that was saved the input object of snp.data-class, the object must be named "data"}
  \item{output_f}{a file will be saved the outputs
		}
  \item{\dots}{further arguments passed to function of fun argument 
		}
}
\author{Unitsa Sangket, Yurii S. Aulchenko and Surakameth Mahasirimongkol}
\examples{
# Example 1 (summit job on R)
################################################
#clear working space
rm(list = ls())

library(GenABEL)
library(ParallABEL)
data(srdta)
data <- srdta[,] 
npro=2 # npro = number of processors
output_f="output_type4_r2fast_c3" 
fun=r2fast

output.p = type4.p(npro,fun,data,output_f=output_f)

# You can load the output from output_type4_r2fast_c3.Rdata
mpi.quit(save="no")

#################################################
}
