\name{type3.p}
\alias{type3.p}
\title{Parallel for the pairwise statistics derived from analyses between each individual }
\description{
	Parallel for pairwise statistics derived from analyses between each individual, for example genome-wide identity-by-state or genomic kinship analyses
}
\usage{
type3.p(npro,fun,data,data_f,...)
}
\arguments{
  \item{npro}{number of processors on the compute-nodes 
		}
  \item{fun}{function name will be processed such as ibs 
		}
  \item{data}{object of snp.data-class}
  \item{data_f}{file name that was saved the input object of snp.data-class, the object must be named "data"}
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
npro=2 # npro = number of processors on the compute-nodes
fun=ibs

output.p = type3.p(npro,fun,data)

output.p[1:5,1:5]

mpi.quit(save="no")

#################################################
}
