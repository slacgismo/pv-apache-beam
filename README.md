# pv-apache-beam

This project is to demostrate how to setup the data pipeline to proces multiple files through the Apache Beam on basic `wordcount` `Solardatatools` and `gridlabd`. It demostrates how to setup and run the pipeline on your local machine and google cloud platform.

## Installation and setup environment

Set up the python virtual environment and install python dependences. In the project folder, the the command below

```
python3 -m venv venv
source ./venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Create a .env file, and set up the google cloud platform cradential json file locations

```
export GOOGLE_APPLICATION_CREDENTIALS='your-cradential-location'
```

### Run Examples in your local machine

### wordcount-example

Under the `wordcount-example` folder, run the command below:

```
python -m wordcount --runner DirectRunner \
--input ./kinglear-1.txt \
--output ./results
```

#### solardatatools-onefile-example

Under the `solardatatools-onefile-example` folder, run the command below:

```
python -m main --runner DirectRunner \
--input ./data \
--output ./parallelism/results \
--direct_num_workers 0
```

#### solardatatools-custom-package-example

In this example, the solardatatools package is packaged as `transformers`. You have to install this custom package by the command.
Under the `solardatatools-custom-package-example` folder, run the command below:

```
pip install ./transformers
```

Then you can execute the command

```
python -m main --runner DirectRunner \
--input ./kinglear-1.txt \
--output ./results
```

#### gridlabd-example

### reference
