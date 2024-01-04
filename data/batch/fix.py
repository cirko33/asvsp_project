import os
from multiprocessing import Pool
import re

def fix(filename):
    text = ""
    print(filename)
    with open("./reviews/" + filename, "r") as f:
        text = f.read()
        text = text.replace("\n\",\"","\",\"")

    with open("./reviews/" + filename, "w") as f:
        f.write(text)

if __name__ == "__main__":
    pool = Pool(processes=10)
    args = os.listdir("./reviews")
    pool.map(fix, args)