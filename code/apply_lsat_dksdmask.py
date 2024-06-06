from __future__ import print_function, division
import sys
from rios import applier, fileinfo
import numpy as np
import csv
import pdb
import argparse
import pandas as pd
import scipy.stats.mstats as mstats
import numpy.ma as ma


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()

    p.add_argument("--imglist", help="Input csv file with list of dbg and dk7 imagery to process")

    cmdargs = p.parse_args()

    if cmdargs.imglist is None:
        p.print_help()
        sys.exit()

    return cmdargs

def apply_dec_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6) | (
                           inputs.dk7_image[0] == 7) | (inputs.dk7_image[0] == 8) | (inputs.dk7_image[0] == 9) | (
                           inputs.dk7_image[0] == 10) | (inputs.dk7_image[0] == 11) | (inputs.dk7_image[0] == 12)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull



def apply_nov_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6) | (
                           inputs.dk7_image[0] == 7) | (inputs.dk7_image[0] == 8) | (inputs.dk7_image[0] == 9) | (
                           inputs.dk7_image[0] == 10) | (inputs.dk7_image[0] == 11)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull




def apply_oct_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6) | (
                           inputs.dk7_image[0] == 7) | (inputs.dk7_image[0] == 8) | (inputs.dk7_image[0] == 9) | (
                           inputs.dk7_image[0] == 10)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_sep_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6) | (
                           inputs.dk7_image[0] == 7) | (inputs.dk7_image[0] == 8) | (inputs.dk7_image[0] == 9)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_aug_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6) | (
                           inputs.dk7_image[0] == 7) | (inputs.dk7_image[0] == 8)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_july_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6) | (
                           inputs.dk7_image[0] == 7)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_june_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5) | (inputs.dk7_image[0] == 6)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull



def apply_may_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4) | (inputs.dk7_image[0] == 5)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_april_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3) | (
                    inputs.dk7_image[0] == 4)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_march_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2) | (inputs.dk7_image[0] == 3)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_feb_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1) | (inputs.dk7_image[0] == 2)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull


def apply_jan_mask(info, inputs, outputs, otherargs):
    """function that applies the mask to Landsat dbg image"""

    mask = (inputs.dk7_image[0] == 1)

    #print(inputs.dk7_image[0])
    #print("mask: ", mask)
    outputs.outimg = inputs.dbg_image.copy()
    nBands = len(inputs.dbg_image)
    for i in range(nBands):
        outputs.outimg[i][mask] = otherargs.valNull



def main_routine(dbgImage, dk7Image, dbgNew, month):
    """Run mainRoutine"""

    # cmdargs = getCmdargs()

    # read in the list of dbg and dk7 imagery to be processed
    # at this stage this needs to be a csv file with two columns and the headers "dbg" and "dk7"
    # with the dbg and dk7 imagery in order!

    # df = pd.read_csv(cmdargs.imglist, header=0)

    # # iterate over the rows and select out the single date dbg and dk7 images to process.
    # for index, row in df.iterrows():
    #     dbgImage = str(row["dbg"])
    #     dk7Image = str(row["dk7"])
    #
    #     # create the new file name
    #     dbgNew = dbgImage[:34] + '_dkbsmask.img'

    # Set up rios to apply images
    controls = applier.ApplierControls()
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()

    # read in the dbg and dk7 images to process
    infiles.dbg_image = dbgImage
    infiles.dk7_image = dk7Image

    otherargs = applier.OtherInputs()
    imginfo = fileinfo.ImageInfo(infiles.dbg_image)
    # set up the nodata values
    otherargs.valNull = imginfo.nodataval[0]
    controls.setStatsIgnore(otherargs.valNull)

    outfiles.outimg = dbgNew

    if month =="01":

        applier.apply(apply_jan_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "02":
        applier.apply(apply_feb_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "03":
        applier.apply(apply_march_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "04":
        applier.apply(apply_april_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "05":
        applier.apply(apply_may_mask, infiles, outfiles, otherargs, controls=controls)

    elif month =="06":

        applier.apply(apply_june_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "07":
        applier.apply(apply_july_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "08":
        applier.apply(apply_aug_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "09":
        applier.apply(apply_sep_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "10":
        applier.apply(apply_oct_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "11":
        applier.apply(apply_nov_mask, infiles, outfiles, otherargs, controls=controls)

    elif month == "12":
        applier.apply(apply_dec_mask, infiles, outfiles, otherargs, controls=controls)
    else:
        print("month unknown")
        import sys
        sys.exit()
    print(dbgNew + ' is complete')


if __name__ == "__main__":
    main_routine()
