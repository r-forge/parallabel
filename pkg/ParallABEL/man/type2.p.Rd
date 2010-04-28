\name{type2.p}
\alias{type2.p}
\title{Parallel for the analyses of statistics of each individual}
\description{
   	Parallel for the analyses of statistics of each individual, for example, summary statistics of genotype quality for each sample 
}
\usage{
type2.p(npro,fun,data,data_f,...)
}
\arguments{
  \item{npro}{number of processors on compute nodes 
		}
  \item{fun}{function name will be processed such as hom 
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
#clear working space
rm(list = ls())
library(GenABEL)
library(ParallABEL)
data(ge03d2.clean)
data <- ge03d2.clean[,]

npro=2 # npro = number of processors
fun=hom

output.p = type2.p(npro,fun,data)

output.p[1:5,]

mpi.quit(save="no")


#################################################
}
