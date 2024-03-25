# Repository of the thesis "Into the crossfire: A Transformer-based approach for detecting gun violence reports online"

This repository contains code and data that were utilized for training Natural Language Processing (NLP) models and conducting analyses in the thesis. 

arXiv paper: [https://arxiv.org/abs/2401.12989](https://arxiv.org/abs/2401.12989)

## Repository Structure
The repository has the following structure:

### 1. `./map`
This directory contains geographical data files:
  - `rio_frontiers.gpkg`: Geospatial data for the frontiers of Rio.
  - `radius_150.gpkg`: Geospatial data within a 150 km radius.
  - `map_rio.qgz`: QGIS project file for the map showing the prototype's search geographical coverage.
  - `metropolitana.gpkg`: Geospatial data for the metropolitan area.

### 2. `./code`
This directory hosts the code used for processing data, creating models, generating visualizations, and more.

#### 2.1 `./code/prototype`
Scripts used in the prototype phase of the project:
  - `backup_airtable.py`: Script to download Airtable data.
  - `update_airtable.py`: Script to update Airtable data.

#### 2.2 `./code/twitter`
Scripts related to data processing from Twitter.

##### 2.2.1 `./code/twitter/process`
  - `dataset_ulang.py`: Script to perform language-specific searches on Twitter data.
  - `dataset_ugeo.py`: Script to perform geographic searches on Twitter data.
  - `fogocruzadorj_replies.py`: Script to process replies from the Fogocruzado RJ Twitter account.
  - `utils.py`: Utility functions for Twitter data processing.
  - `group_interactions_fogocruzado.ipynb`: Jupyter notebook to group interactions on Fogocruzado Twitter data and facilitate data visualisation.

##### 2.2.2 `./code/twitter/get`
  - `fogocruzado_replies.py`: Script to get replies to Fogocruzado tweets.
  - `geo_search.py`: Script to perform geographic searches on Twitter data.
  - `broad_search.py`: Script to perform broad searches on Twitter data.

#### 2.3 `./code/train_test_sets`
Scripts to prepare training and testing sets.
  - `h_interactions.ipynb`: Jupyter notebook to generate the test set for manual annotation.
  - `l_train.ipynb`: Jupyter notebook to prepare the training set.
  - `h_reports.ipynb`: Jupyter notebook to prepare the raw testing set.
  - `u_unlabel.ipynb`: Jupyter notebook to prepare unlabeled data.

#### 2.4 `./code/dataviz`
Scripts and Jupyter notebooks for the figures in the thesis.

#### 2.5 `./code/nlp`
Scripts and Jupyter notebooks for Natural Language Processing tasks and related models.

#### 2.6 `./code/intervention`
  - `diff_in_diff.ipynb`: Jupyter notebook for conducting Difference-in-Differences (DiD) analysis.

### 3. `./figs`
This directory contains all figures and plots generated from the data.
### 4. `./data`
Data is categorized into subfolders according to the data source, such as Twitter and Fogo Cruzado. Data generated or used by the scripts are into the folder my_intervention.

## Usage
Refer to `pyproject.toml` for the required Python version and package dependencies to generate the data visualisation and figures.
