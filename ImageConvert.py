from PIL import Image, ImageOps, ImageDraw, ImageFont
from PIL import PSDraw
import os, datetime
from os import listdir
from os.path import isfile, join
from pathlib import Path
from rembg import remove

size = (100, 150)
with Image.open("btrust_logo.png") as im:
    # create an image
    #out = Image.new("RGB", (150, 100), (255, 255, 255))
    # get a font
    #fnt = ImageFont.truetype("Pillow/Tests/fonts/FreeMono.ttf", 40)
    # get a drawing context
    d = ImageDraw.Draw(im)
    # draw multiline text
    d.multiline_text((50, 50), "Hello\nWorld", fill=(0, 0, 0))

    im.show()

def createFolder() -> str:
    directory = os.path.join(os.getcwd(), datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))
    try:
        os.makedirs(directory)
    except:
        print("Create folder failed.")
        return ""
    return directory

def readFiles():
    mypath = "ProductImage"
    # mypath = "Veg&Fruit"
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    return onlyfiles

def convertImage(file: str, printFileName: str, folder: str):
    font = ImageFont.truetype("arial.ttf", 850)
    with Image.open("ProductImage\\" + file) as im:
        #im = remove(im)
        imageDrawing = ImageDraw.Draw(im)
        imageDrawing.multiline_text((50, 50), printFileName, fill=(0, 0, 0), font=font)
        im = im.resize((80, 80))
        
        im = im.convert('RGB')
        im.save(folder + "\\" + file[:-3] + "bmp")

def nomalizeFileName(file: str) -> str:
    fileName = Path(file).stem.lstrip("0")
    if fileName and fileName[0: 2] == "20" and len(fileName) > 5:
        return fileName[2: 6]
    elif fileName and fileName[0] == "2" and len(fileName) > 5:
        return fileName[1: 6]
    elif fileName and len(fileName) > 6:
        return ""
    return fileName

def saveFile(file: str, folder: str):
    with Image.open("ProductImage\\" + file) as im:
        im.save(folder + "\\" + file)

def saveFile2(file: str, folder: str):
    with Image.open("Veg&Fruit\\" + file) as im:
        im.save(folder + "\\0" + file)

if __name__ == "__main__":
    # create folder with timestamp
    folder = createFolder()
    if folder == "":
        print("bye")
        exit
    print(f"Create folder {folder} success.")
    # read files one by one then store them in new folder
    files = readFiles()
    for file in files:
        #saveFile2(file, folder)
        printFileName = nomalizeFileName(file)
        if printFileName != "":
            convertImage(file, printFileName, folder)
        else:
            saveFile(file, folder)
