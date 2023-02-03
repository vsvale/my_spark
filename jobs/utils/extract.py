def read_json(spark,filepath):
    """
    load data from json file format

    :param spark: spark session object
    :param filename: location of the file
    :return: spark dataframe
    """

    df = spark.read.json(filepath)
    return df