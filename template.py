"""Database configuration template
====================================

This script provides you a example of self-defined dataloader 
to construct your own local database. You need to know the 
common phases in constructing local database.

1. Extracting data from datasource
2. Preprocessing data before pouring them into database
3. Import data into database
4. Postpross after import

The four steps in correlated with four functions: read, 
preprocess, write, postprocess.

There are two things you need to do: 1. Define a Loader class 
based on ba.Loader; 2. Set a config in a dict.

There are several loaders predefine in the bearalpha, however,
things may never be enough. We provided the examples of self-
defined loader.

And you can store the loader and the config into bearalpha's
cache, just make sure that you name the loader with 'Loader'
in it and name the config with 'baconfig' in it. Then run the 
shell command `ba store <your file path in module fromat>`, like
`ba store database.dataloader`.
"""


import bearalpha as ba


# self defined data loader
class MyLoader(ba.Loader):
    # functions you must accomplish
    # 1. self.read; 2. self.write
    # functions you may want to accomplish
    # 1. self.preprocess; 2. self.postprocess
    # all of them takes no argument, to get
    # the input argument, use self.<fucntion name>args instead

    def read(self):
        # get argument by self.readargs
        pass

    def write(self):
        # get argument by self.writeargs
        pass

    def preprocess(self):
        # get argument by self.preprocessargs
        pass

    def postprocess(self):
        # get argument by self.postprocessargs
        pass


# config name must contains `baconfig` or it will not be recognized
baconfig_name = dict(
    # any loader you use, current support for ba.Local
    loader = ba.Local, 
    # RECOMMEND: your database path stored in ba.Cache(),
    # or any string type path to your database file / service
    database = "path or connect string to your database",
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


if __name__ == "__main__":
    baconfig_name['loader'](baconfig_name)()