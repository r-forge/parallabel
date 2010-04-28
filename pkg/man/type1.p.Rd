\name{type1.p}
\alias{type1.p}
\title{Parallel for the analyses of statistics of each SNP}
\description{
	Parallel for the analyses of statistics of each SNP, such as SNP characterization statistics or association test statistics 
}
\usage{
type1.p(npro,fun,data,data_f,...)
}
\arguments{
  \item{npro}{number of processors on compute nodes 
		}
  \item{fun}{function name will be process such as mlreg.p 
		}
  \item{data}{object of snp.data-class}
  \item{data_f}{file name that was saved the input object of snp.data-class, the object must be named "data"}
  \item{\dots}{further arguments passed to function of fun argument 
		}
}
\author{Unitsa Sangket, Yurii S. Aulchenko and Surakameth Mahasirimongkol}
\examples{
#Example 1 (summit job on R)
################################################
#clear working space
rm(list = ls())
library(GenABEL)
library(ParallABEL)
formula=dm2~sex+age
data(ge03d2.clean)
data <- ge03d2.clean[,]
npro=2 # npro = number of processors on the compute-nodes
fun=mlreg.p

output.p = type1.p(npro,fun,data,formula=formula)

output.p[1:5,]

mpi.quit(save="no")

#################################################

}

