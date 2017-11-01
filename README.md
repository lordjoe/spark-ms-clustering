# spark-ms-clustering

# Introduction

The spark-ms-clustering  application is the [Spark Algorithm](https://spark.apache.org/) of the newly developed _mass spectra clustering_ algorithm. It is used to cluster massive amount
of mass spectrometry data in public resources such as the [PRIDE](https://www.ebi.ac.uk/pride) repository for MS/MS based proteomics data.

The spark-ms-clustering application relies on the [spectra-cluster](https://github.com/spectra-cluster/spectra-cluster) clustering API. All implementations of relevant clustering algorithms
can be found there.

The following descriptions are only based on the clustering pipeline used to create the [PRIDE Cluster](https://www.ebi.ac.uk/pride/cluster) resource.


# Getting help

If you have questions or need additional help, create an issue: https://github.com/bigbio/spark-ms-clustering/issues


# Giving your feedback

Please give us your feedback, including error reports, suggestions on improvements, new feature requests. You can do so by opening a new issue at our [issues section](https://github.com/spectra-cluster/spectra-cluster-hadoop/issues)

# How to cite

Please cite this library using one of the following publications:

- Recognizing millions of consistently unidentified spectra across hundreds of shotgun proteomics datasets.
  Griss J, Perez-Riverol Y, Lewis S, Tabb DL, Dianes JA, Del-Toro N, Rurik M, Walzer MW, Kohlbacher O, Hermjakob H, Wang R, Vizcaíno JA.
  Nat Methods. 2016 Aug;13(8):651-656. Epub 2016 Jun 27. [PDF](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4968634/pdf/emss-68835.pdf)

# Contribute
We welcome all contributions submitted as [pull](https://help.github.com/articles/using-pull-requests/) request.

# License
This project is available under the [Apache 2](http://www.apache.org/licenses/LICENSE-2.0.html) open source software (OSS) license.
