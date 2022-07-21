import bearalpha as ba

# config name must contains `baconfig` or it will not be recognized
baconfig_name = dict(
    # any loader you use, current support for ba.Local
    loader = ba.Local, 
    # RECOMMEND: your database path stored in ba.Cache(),
    # or any string type path to your database file / service
    database = ba.Cache().get('local'),
    # table you want to store data in
    table = "table_name",
    # OPTIONAL: keyword arguments for preprocess function
    preprocessargs = dict(),
    # OPTIONAL: keyword arguments for read function
    readargs = dict(),
    # OPTIONAL: keyward arguments for write function
    writeargs = dict(),
    # OPTIONAL: keyword arguments for postprocess function
    posprocessargs = dict(),
)