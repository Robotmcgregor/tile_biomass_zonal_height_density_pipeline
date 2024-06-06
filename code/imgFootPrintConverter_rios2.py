#!/usr/bin/env python

"""
Script to convert other raster layers to the same foot print and projection as
the referece raster layer.

Author: Grant Staben
Date:23/02/2017
"""

from __future__ import print_function, division
import sys
import argparse
from rios import applier
import pdb
from rios import fileinfo


# def getCmdargs():
#     """
#     Get command line arguments
#     """
#     p = argparse.ArgumentParser()
#     p.add_argument("--reffile", help="Image to use as the reference image")
#     p.add_argument("--infile", help="input the file to be converted")
#     p.add_argument("--outfile", help="name for the converted file")
#     cmdargs = p.parse_args()
#
#     if cmdargs.reffile is None:
#         p.print_help()
#         sys.exit()
#
#     return cmdargs

def copyFootPrint(info, inputs, outputs):
    
    """Function to be called by rios to convert the input image"""
    copy = inputs.image2
    # places the result in the outputs as outimage.
    outputs.outimage = copy 

def main(reffile, infile, outfile):
    """Main routine"""
    
    # cmdargs = getCmdargs()
    
    # Set up input and output filenames.
    infiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()

    infiles.image1 = reffile
    infiles.image2 = infile
    
    # set the reference image which is the image foot print you are emulating 
    controls.setReferenceImage(infiles.image1)

    outfiles = applier.FilenameAssociations()
    outfiles.outimage = outfile

    # Apply the function to the inputs, creating the outputs.
    applier.apply(copyFootPrint, infiles, outfiles, controls=controls)
    
if __name__ == "__main__":
    main()