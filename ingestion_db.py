{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "authorship_tag": "ABX9TyOBUDIne6zfuTlJaMLxYzHT",
      "include_colab_link": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/rushikeshtak17/Vendor-Performance-Analytics-End-to-End-Solution-Using-SQL-Python-and-PowerBI/blob/main/ingestion_db.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": 1,
      "metadata": {
        "id": "GwOtcDblj6zb"
      },
      "outputs": [],
      "source": [
        "import pandas as pd\n",
        "import os\n",
        "from sqlalchemy import create_engine\n",
        "import logging\n",
        "import time\n",
        "\n",
        "logging.basicConfig(\n",
        "    filename = \"logs/ingestion_db.log\",\n",
        "    level=logging.DEBUG,\n",
        "    format=\"%(asctime)s - %(levelname)s - %(message)s\",\n",
        "    filemode=\"a\"\n",
        ")\n",
        "\n",
        "engine = create_engine('sqlite:///inventory.db')\n",
        "\n",
        "'''INSERT CONTINUOUS DATA FRAME INTO DATABASE TABLE'''\n",
        "def ingest_db(df, table_name, engine):\n",
        "    df.to_sql(table_name, con = engine, if_exists = 'replace', index=False,chunksize=100000)\n",
        "\n",
        "def load_raw_data():\n",
        "    '''This function will load the CSVs as dataframe and ingest into db'''\n",
        "    start = time.time()\n",
        "    for file in os.listdir(r'/Data'):\n",
        "                           if '.csv' in file:\n",
        "                               full_path = os.path.join(r'/Data', file)\n",
        "                               df = pd.read_csv(full_path)\n",
        "                               logging.info(f'Ingesting {file} in db')\n",
        "                               ingest_db(df, file[:-4], engine)\n",
        "\n",
        "    end = time.time()\n",
        "    total_time = (end - start)/60\n",
        "\n",
        "    logging.info('------------Ingestion Complete------------')\n",
        "    logging.info(f'\\nTotal TIme Taken: {total_time} minutes')\n",
        "\n",
        "if __name__=='__main__':\n",
        "    load_raw_data()"
      ]
    },
    {
      "cell_type": "code",
      "source": [],
      "metadata": {
        "id": "sW1Dkv3Ps0PA"
      },
      "execution_count": null,
      "outputs": []
    }
  ]
}