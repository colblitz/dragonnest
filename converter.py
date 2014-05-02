import os
from moviepy.editor import *

def convert(filename):
    mp4path = filename + ".mp4"
    gifpath = filename + ".gif"
    print mp4path
    print gifpath
    if not os.path.isfile(gifpath):
        VideoFileClip(mp4path).resize(0.6).to_gif(gifpath)

for file in os.listdir("./"):
    if file.endswith(".mp4"):
        filename = os.path.splitext(file)[0]
        convert(filename)
