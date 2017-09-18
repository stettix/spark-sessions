# Spark sessionization

This repo contains some examples of different ways to divide records into sessions by timestamps and a maximum
interval between records of a session.

For a detailed discussion, see the blog post at [http://www.janvsmachine.net/2017/09/sessionization-with-spark.html](http://www.janvsmachine.net/2017/09/sessionization-with-spark.html).

## Implementation notes

The code reads example click data based on the [Outbrain Click Prediction](https://www.kaggle.com/c/outbrain-click-prediction) dataset
from Kaggle. This comes as a very large zip-compressed CSV file (`page_views.csv.zip`). I converted this to Parquet format and partitioned it for efficiency.
Thus, the code assumes Parquet format with a suitable schema for its input. Otherwise the data is untouched.

The [/scripts](/scripts) folder contains scripts that create a suitable EMR cluster in AWS, and that runs the three different
implementations as EMR steps. No attempt has been made to make these completely general, so you'll have to hack these
to match your own AWS setup if you want to run these.
