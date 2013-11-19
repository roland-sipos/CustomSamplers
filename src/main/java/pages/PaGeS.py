#!/usr/bin/env python

import getopt, os, sys
import hashlib
import random
import socket
import uuid
import time
import pylab

from os import listdir
from os.path import isfile, join


def ensure_dir(f):
    if not os.path.exists(f):
        os.makedirs(f)

def hashfile(f, hasher, blocksize=65536):
    buf = f.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = f.read(blocksize)
    return hasher.hexdigest()

def getCategory(id = 0):
    if (id == 0):
        return "SMALL"
    elif (id == 1):
        return "MEDIUM"
    #elif (id == 2):
    #    return "LARGE"
    #elif (id == 3):
    #    return "HUGE"

def generateRandomBinary(destpath, size, nofiles, split_after, inputFile=None):
    print "Generating random binary files ..."
    print "  <> Destination: " + destpath
    if (inputFile is not None):
        print "  <> Input file will be used: " + inputFile
    else:
        print "  <> Num. of files: " + str(nofiles)
        print "  <> Size of files will be: " + str(size) + " [Bytes]"
    ensure_dir(destpath)

    if (inputFile is not None):
        content = None
        with open(inputFile) as f:
            content = f.readlines()
        lineNum = 0
        for cat in range(0, 2):
            controlLine = content[lineNum].split()
            start = lineNum + 1
            stop = lineNum + int(controlLine[1]) + 1
            lineNum += int(controlLine[1]) + 1
            for no in range(start, stop):
                l = content[no].split()
                filename = destpath + "/rbinary-" + getCategory(cat) + "-" + str(no - start + 1) + ".bin"
                sizeInByte = float(l[1]) * 1048576.0
                print "    -> Generating: " + filename
                print "       With size: " + str(float(l[1])) + " [MB]"
                with open(filename, 'wb') as fout:
                    fout.write(os.urandom(int(round(sizeInByte)))) #int(l[1]) * 1048576))

    else:
        for x in range(0, int(nofiles)):
            filename = destpath + "/rbinary-" + str(x + 1) + ".bin"
            print "    -> Generating: " + filename
            with open(filename, 'wb') as fout:
                fout.write(os.urandom(int(size)))
        if (split_after):
            print "Splitting after generation currently not implemented!"


def splitBinaries(path, chunksize, whichhash, tagname):
    print "Splitting binary files ..."
    print "  <> ... under path: " + path
    print "  <> For TAG_NAME: " + tagname
    print "  <> Chunk size: " + str(chunksize) + " [Bytes]"
    print "  <> Checksums will be generated with: " + whichhash
    if not os.path.exists(path):
        print "ERROR: The requested path to files does not exist!"
        sys.exit(2)

    hasher = None
    if (whichhash == "MD5"):
      hasher = hashlib.md5()
    elif (whichhash == "SHA1"):
      hasher = hashlib.sha1()
    elif (whichhash == "SHA128"):
      hasher = hashlib.sha128()
    elif (whichhash == "SHA256"):
      hasher = hashlib.sha256()
    elif (whichhash == "SHA512"):
      hasher = hashlib.sha512()

    filelist = [ f for f in listdir(path) if isfile(join(path, f)) ]
    print filelist
    for f in filelist:
        dirForChunks = join(path, f) + ".chunks"
        ensure_dir(dirForChunks)
        print "    -> Splitting " + f + " into " + dirForChunks
        fop = open(join(path, f), 'rb')
        data = fop.read()
        fop.close()
        bytes = len(data)
        noOfChunks = bytes / chunksize
        if (bytes % chunksize):
            noOfChunks += 1
        print "    -> Generating META information ..."
        metaFileName = "META-" + f + ".info"
        streamerMetaFN = "STREAMER_INFO.bin"
        metaFilePath = join(dirForChunks, metaFileName)
        streamerMetaFP = join(dirForChunks, streamerMetaFN)
        metaFile = open(metaFilePath, 'w')
        metaFile.write("tag_name " + tagname + "\n")
        metaFile.write("object_type RANDOM\n")
        metaFile.write("version v" + str(random.randint(1, 10000)) + "\n")
        metaFile.write("since " + str(int(round(time.time() * 1000))) + "\n")
        metaFile.write("cmssw_release CMSSW_" + str(random.randint(0, 9))
                                        + "_" + str(random.randint(0, 9)) + "_X\n")
        hashOfFile = hashfile(open(join(path, f), 'rb'), hasher)
        print "    -> " + whichhash + ": " + hashOfFile

        metaFile.write("hash_type " + whichhash + "\n")
        metaFile.write("payload_hash " + hashOfFile + "\n")
        with open(streamerMetaFP, 'wb') as fout:
            fout.write(os.urandom(int(20480))) # 100k meta blob

        chunkNames = []
        chunkNo = 1
        for i in range (0, bytes, chunksize):
            chunkFName = "chunk-%s.bin" % str(chunkNo)
            chunkNames.append(chunkFName)
            cfPath = join(dirForChunks, chunkFName)
            cf = open(cfPath, 'wb')
            cf.write(data[i:i+chunksize])
            cf.close()
            hashOfChunk = hashfile(open(cfPath, 'rb'), hasher)
            metaFile.write(str(chunkNo) + " " + hashOfChunk + "\n")
            print "      + Created " + str(chunkNo) + ". chunk. " + whichhash + ":" + hashOfChunk
            chunkNo += 1;

        metaFile.close()


def joinChunks():
    print "Unsupported yet."

def randomizeFileSizes(numOf, avg, var, outFile):
    print "Randomizing file category numbers and sizes ..."
    print "  <> ... output will be in file: " + outFile + ".txt"
    print "  <> ... histogram will be in file: " + outFile + ".pdf"
    print "  <> ... Gauss dist. randoms will use avg: " + str(avg) + " and variance: " + str(var)
    print "  <> ... number of random sizes will be generated: " + str(numOf)
    print "  <> ... categories are the following: "
    print "         (AVG Mb, VAR): small (10-3), normal (25-6), large (50-6), huge (80-6)"

    realRandomVariates = [[], []] ##[]] #lists for small, normal, large and huge sizes
    for i in range(int(numOf)):
        newValue = random.gauss(avg, var)
        if (newValue < 1):
          realRandomVariates[0].append(random.gauss(10, 2))
        elif (newValue >= 1 and newValue < 2):
          realRandomVariates[0].append(random.gauss(15, 2))
        elif (newValue >= 2 and newValue < 3):
          realRandomVariates[1].append(random.gauss(25, 2))
        elif (newValue >= 3): # 3rd cat
          realRandomVariates[1].append(random.gauss(30, 2)) # HUGE: 80, 6

    oF = open(outFile + ".txt", 'w')
    for a in range(0, 2):
        cat = getCategory(a)
        numOfCat = len(realRandomVariates[a])
        oF.write(cat + " " + str(numOfCat) + "\n")
        print cat + ": " + str(numOfCat)
        sizesum = 0
        for b in range(0, numOfCat):
            sizesum += realRandomVariates[a][b]
            oF.write(str(uuid.uuid4()) + " " + str(realRandomVariates[a][b]) + "\n")
        print "  -> sum of size for this category is: " + str(sizesum) + " [MB]"
        pylab.hist(realRandomVariates[a], bins=60, range=(0, 70), histtype='step', align='mid')

    oF.close()

    pylab.xlabel('Size range [Mb]')
    pylab.ylabel('Count')

    outFilePdf = outFile + ".pdf"
    pylab.savefig(outFilePdf)
    pylab.show()


def usage():
    print "#######################################################################"
    print "#                 ------------------------------                      #"
    print "#                 PaGeS - PayloadGeneratorScript                      #"
    print "#                 ------------------------------                      #"
    print "#                                                                     #"
    print "# This application can create random binary files with dedicated      #"
    print "# sizes, destinations and numbers. Also can split files under the     #"
    print "# marked directory, treating them as binary, into chunks with a given #"
    print "# chunk size.                                                         #"
    print "#                                                                     #"
    print "# You can use the following arguments of the sub-programs:            #"
    print "# Random Generator:                                                   #"
    print "#   -n --numberof     : number of files to randomize |default: 5      #"
    print "#   --mean            : mean of category Gauss dist. |default: 1.35   #"
    print "#   --variance        : var. of category Gauss dist. |default: 1.75   #"
    print "#   --randMetaFile    : the output file of random sizes, infos        #"
    print "#                       |default: randomVariates.txt                  #"
    print "#   --randomize       : start randomization                           #"
    print "#                                                                     #"
    print "# File Generator:                                                     #"
    print "#   -h --help         : to get this message                           #"
    print "#   -s --size         : size of each random binary   |default: 100MB  #"
    print "#   -d --destination  : directory of output files    |default: ./out  #"
    print "#   -n --numberof     : number of files to generate  |default: 5      #"
    print "#   --split-after     : split after generation       |default: false  #"
    print "#   -i --input        : or choose an input file of random sizes       #"
    print "#   -g --generate     : start generation                              #"
    print "#                                                                     #"
    print "# File Splitter:                                                      #"
    print "#   -c --chunksize    : size of chunks to split to   |default: 10MB   #"
    print "#   -p --path         : location of files to split   |default: ./out  #"
    print "#   --checksum        : which hash to use            |default: SHA1   #"
    print "#                     : |supported: MD5, SHA256, SHA512               #"
    print "#   --split           : split the files under path                    #"
    print "#                                                                     #"
    print "#######################################################################"

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:d:n:ri:gc:p:",
            ["help", "size=", "destination=", "numberof=", 
             "mean=", "variance=", "randMetaFile=", "randomize", 
             "input=", "generate", "chunksize=", "path=", "checksum=", "split"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    output = None
    verbose = False
    size = 104857600 #default file size is 100MB
    nofiles = 5 #default number of files is 5
    mean = 1.35
    variance = 1.75
    randMetaFile = "randomVariates"
    inputFile = None
    destpath = "./out" #default destination dir is ./out
    split_after = False
    tagName = socket.gethostname()
    path = "./out"
    chunksize = 10485760
    whichhash = "SHA1"
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-s", "--size"):
            size = a
        elif o in ("-d", "--destination"):
            destpath = a
        elif o in ("-n", "--numberof"):
            nofiles = a
        elif o in ("--mean"):
            mean = a
        elif o in ("--variance"):
            variance = a
        elif o in ("--randMetaFile"):
            randMetaFile = a
        elif o in ("-r", "--randomize"):
            randomizeFileSizes(nofiles, mean, variance, randMetaFile)
        elif o in ("-i", "--input"):
            inputFile = a
        elif o in ("-g", "--generate"):
            generateRandomBinary(destpath, size, nofiles, split_after, inputFile)
        elif o in ("-c", "--chunksize"):
            chunksize = int(a)
        elif o in ("-p", "--path"):
            path = a
        elif o in ("--checksum"):
            whichhash = a
        elif o in ("--split"):
            splitBinaries(path, chunksize, whichhash, tagName)
        else:
            assert False, "unhandled option"


if __name__ == "__main__":
    main()


