# Feature engineering pipeline

## fea_eng_enc.py content instruction

PreCLs class:

1. _fill_na function: fill na based on feature dictionary.
2. _ranker function: which ranks the order across the row which has optimal complexity.
3. _min_max function: which normalize the dataframe using sklearn min max.
4. _zscal_ranker: which normalize the dataframe with z scaling method and added ranked features.
5. _pipeline: function which calls all the preprocessing steps based on the required arg.


## Workflow
```puml
daily_tb -> prepro_pipeline
prepro_pipeline -> imputation
imputation -> encoding
encoding -> scaling
scaling -> prepro_daily_tb
```
1. load daily data, load the feature dictionary
2. call the static method `_fill_na` from `PreCls` to impute `nan`
3. start 'PreCLs' object
4. call '_pipe_line' method from the 