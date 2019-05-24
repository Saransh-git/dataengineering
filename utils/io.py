from io import FileIO, BufferedRandom, TextIOWrapper
from pathlib import Path
from itertools import islice


path = Path("~/Desktop/pigsample.txt")
path = path.expanduser()  # expands ~ and ~user construct in file path

is_created = False
f = None
if path.exists():
    print("woah")
    f = path.open(mode='ab+')  # buffering is turned off with buffering = 0
else:
    is_created = True
    f = path.open(mode='xb')  # buffering is turned off with buffering = 0

if isinstance(f, FileIO):
    print("open returns an instance of FileIO")  # only when the raw object is opened up with buffering turned off
    # otherwise for bytes BufferedWriter instance is returned, otherwise for text mode TextIOWrapper is returned.

f.seek(0)
for line in f:
    print(line)

if is_created:
    path.unlink()  # remove the file

f.seek(0)
lines = list(islice(f, 2))
print(lines)

lines = list(islice(f, 1))
print(lines)
