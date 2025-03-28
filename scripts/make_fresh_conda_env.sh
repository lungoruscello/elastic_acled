#!/bin/sh

#####################################
# Install a fresh, notebook-enabled conda
# environment with pandas and elasticsearch
#####################################

NEW_ENV_NAME="pandas_elastic"
CONDA_BASE=$(conda info --base | grep -o "/.*")
source $CONDA_BASE/etc/profile.d/conda.sh  # make `conda activate` available to sub-shells

conda create -n $NEW_ENV_NAME jupyter pandas tqdm ipython=8.* -y
conda activate $NEW_ENV_NAME
conda config --set channel_priority true

conda install "elasticsearch=8.17.*" pyarrow -c conda-forge -y
