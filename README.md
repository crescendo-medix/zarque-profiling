[![PyPI version](https://badge.fury.io/py/zarque-profiling.svg)](https://badge.fury.io/py/zarque-profiling)
![Python Versions](https://img.shields.io/pypi/pyversions/zarque-profiling.svg)
![PyPI - Status](https://img.shields.io/pypi/status/zarque-profiling)
![GitHub](https://img.shields.io/github/license/crescendo-medix/zarque-profiling)
[![Downloads](https://static.pepy.tech/badge/zarque-profiling)](https://pepy.tech/project/zarque-profiling)
[![Downloads](https://static.pepy.tech/badge/zarque-profiling/month)](https://pepy.tech/project/zarque-profiling)
[![Downloads](https://static.pepy.tech/badge/zarque-profiling/week)](https://pepy.tech/project/zarque-profiling)

<p align="center">
<img src="https://user-images.githubusercontent.com/132550577/236602881-83d833df-01bc-4012-a42e-11d93846bbfa.png" alt="Zarque-profiling">
</p>

Zarque-profiling is a data profiling tool that is 3x faster than Pandas-profiling. Zarque-profiling offers a new option for your big data profiling needs. 

### Features

Zarque-profiling has the same features, analysis items, and output reports as Pandas-profiling, with the ability to perform minimal-profiling (minimal=True), maximal-profiling (minimal=False), and the ability to compare two reports.  

>*Note:*    
*For big data, it is not recommended to use maximal-profiling (minimal=False) because of the time required for the analysis process. Minimal-profiling (minimal=True) is set as the default.*  


### Powered by Polars

Zarque-profiling is based on pandas-profiling (ydata-profiling) and uses Polars instead of Pandas to speed up the analysis process.  

###  Use cases

- Profiling large datasets as a standalone package  
  Profiling of large data sets that Pandas-profiling is too time consuming to handle.  
  Data profiling when Polars is used for data analytics and data science.  
- Seamless integration with existing packages  
- EDA (Exploratory Data Analysis)  
  Simple data analysis without writing code. (histograms, scatter plots, heat maps, text analysis)  
- Comparing multiple version of the same dataset (profiling reports)  
  Compare data before and after data wranglin.  
  Compare training data with evaluation data by machine learning.  
- Data preparation/Data migration solution business  
  Estimation of man-hours required.  
  Help to create data specification.  
  Determine if dataset should be migrated or not.  


***

### Benchmark

The figure below shows the benchmark results of data acquisition and analysis processing time for 1 million to 100 million rows in minimal profiling (minimal=True). This data is for reference only. Processing times vary depending on the performance of the PC used and the amount of memory.  

<p align="center">
<img src="https://user-images.githubusercontent.com/132550577/236175318-f7f34294-b7cd-48ab-b13b-acfc4cc3e442.png">
</p>

***

### Installation

You can install using the `pip` package manager.

```sh
pip install zarque-profiling
```

### How to use

... *See Pandas-profiling for details on usage.*  

Prepare Polars data-frame.

```py
import polars as pol
# CSV file
df = pol.read_csv("path/file_name.csv")
# Parquet file
df = pol.read_parquet("path/file_name.parquet")
```

Generate the standard profiling report.  

```py
from zarque_profiling import ProfileReport
# Minimal-profiling
ProfileReport(df, title="Zarque Profiling Report")
# Maximal-profiling
ProfileReport(df, minimal=False, title="Zarque Profiling Report")
```

Using inside Jupyter Lab.  

```py
report = ProfileReport(df)
# Displaying the report as a set of widgets
report.to_widgets()
# Directly embedded in a cell
report.to_notebook_iframe()
```

Exporting the report to a file.  

```py
# As a HTML file
report.to_file("path/file_name.html")
# As a JSON file
report.to_json("path/file_name.json")
```

Compare 2 profiling reports.  

```py
from zarque_profiling import compare
df1 = pol.read_csv("path/file_name.csv")
df2 = pol.read_csv("path/file_name_corrected.csv")
report1 = ProfileReport(df1, title="Original Data")
report2 = ProfileReport(df2, title="Corrected Data")
compare([report1, report2])
```

### Customize examples  

>*For big data, the following code example takes long time for the analysis process.*

Correlation Diagram (spearman, pearson, phi_k, cramers and kendall).  

```py
ProfileReport(
    df,
    minimal=False,
    correlations={
        "spearman": {"calculate": True, "warn_high_correlations": True, "threshold": 0.9},
        "pearson" : {"calculate": True, "warn_high_correlations": True, "threshold": 0.9},
        "phi_k"   : {"calculate": True, "warn_high_correlations": True, "threshold": 0.9},
        "cramers" : {"calculate": True, "warn_high_correlations": True, "threshold": 0.9},
        "kendall" : {"calculate": True, "warn_high_correlations": True, "threshold": 0.9},
        "auto"    : {"calculate": False, "warn_high_correlations": False, "threshold": 0.9}
    }
)
```

Text analysis (length distribution, word distribution and character information).  

```py
ProfileReport(
    df,
    vars={"cat": {"length": True, "words": True, "characters": True}}
)
```

### License

- MIT license  
