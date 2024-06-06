#!/usr/bin/env python

"""
Fractional cover zonal statistics pipeline
==========================================

Description: This pipeline comprises of 12 scripts which read in the Rangeland Monitoring Branch odk instances
{instance names, odk_output.csv and ras_odk_output.csv: format, .csv: location, directory}
Outputs are files to a temporary directory located in your working directory (deleted at script completion),
or to an export directory located a the location specified by command argument (--export_dir).
Final outputs are files to their respective property sub-directories within the Pastoral_Districts directory located in
the Rangeland Working Directory.


step1_1_initiate_fractional_cover_zonal_stats_pipeline.py
===============================
Description: This script initiates the Fractional cover zonal statistics pipeline.
This script:

1. Imports and passes the command line arguments.

2. Creates two directories named: user_YYYYMMDD_HHMM. If either of the directories exist, they WILL BE DELETED.

3. Controls the workflow of the pipeline.

4. deletes the temporary directory and its contents once the pipeline has completed.


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 1.0

###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the 'Software'), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

##################################################################################################

===================================================================================================

Command arguments:
------------------

--tile_grid: str
string object containing the path to the Landsat tile grid shapefile.

--directory_odk: str
string object containing the path to the directory housing the odk files to be processed - the directory can contain 1
to infinity odk outputs.
Note: output runtime is approximately 1 hour using the remote desktop or approximately  3 hours using your laptop
(800 FractionalCover images).

--export_dir: str
string object containing the location of the destination output folder and contents(i.e. 'C:Desktop/odk/YEAR')
NOTE1: the script will search for a duplicate folder and delete it if found.
NOTE2: the folder created is titled (YYYYMMDD_TIME) to avoid the accidental deletion of data due to poor naming
conventions.

--image_count
integer object that contains the minimum number of Landsat images (per tile) required for the fractional cover
zonal stats -- default value set to 800.

--landsat_dir: str
string object containing the path to the Landsat Directory -- default value set to r'Z:\Landsat\wrs2'.

--no_data: int
ineger object containing the Landsat Fractional Cover no data value -- default set to 0.

--rainfall_dir: str
string object containing the pathway to the rainfall image directory -- default set to r'Z:\Scratch\mcintyred\Rainfall'.

--search_criteria1: str
string object containing the end part of the filename search criteria for the Fractional Cover Landsat images.
-- default set to 'dilm2_zstdmask.img'

--search_criteria2: str
string object from the concatenation of the end part of the filename search criteria for the Fractional Cover
Landsat images. -- default set to 'dilm3_zstdmask.img'

--search_criteria3: str
string object from the concatenation of the end part of the filename search criteria for the QLD Rainfall images.
-- default set to '.img'

======================================================================================================

"""

# Import modules
from __future__ import print_function, division
import os
from datetime import datetime
import argparse
import shutil
import sys
import warnings
import glob
import pandas as pd
import geopandas

warnings.filterwarnings("ignore")


def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')

    p.add_argument('-d', '--data', help='The directory the site points csv file.')

    p.add_argument('-t', '--tile_grid',
                   help="Enter filepath for the Landsat Tile Grid.shp.",
                   default=r"N:\Landsat\tilegrid\Landsat_wrs2_TileGrid.shp")

    p.add_argument('-x', '--export_dir',
                   help='Enter the export directory for all of the final outputs.',
                   default=r'U:\scratch\rob\pipelines\outputs')

    p.add_argument('-i', '--image_count', type=int,
                   help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
                   default=100)

    p.add_argument('-l', '--lsat_dir', help="The wrs2 directory containing landsat data",
                   default=r"N:\Landsat\wrs2")

    p.add_argument('-n', '--no_data', help="Enter the Landsat Fractional Cover no data value (i.e. 0)",
                   default=0)

    p.add_argument('-p', '--path', help="Enter the Landsat path (i.e. 106)",
                   default=0)

    p.add_argument('-r', '--row', help="Enter the Landsat row (i.e. 069)",
                   default=0)

    p.add_argument('-z', '--zone', help="Enter the Landsat tile zone (i.e. 2 or 3)",
                   default=0)

    cmd_args = p.parse_args()

    if cmd_args.data is None:
        p.print_help()

        sys.exit()

    return cmd_args


def temporary_dir_fn():
    """ Create a temporary directory 'user_YYYMMDD_HHMM'.

    @return temp_dir_path: string object containing the newly created directory path.
    @return final_user: string object containing the user id or the operator.
    """

    # extract user name
    home_dir = os.path.expanduser("~")
    _, user = home_dir.rsplit('\\', 1)
    final_user = user[3:]

    # create file name based on date and time.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    temp_dir_path = '\\' + str(final_user) + '_' + str(date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message zzzz.
    try:
        shutil.rmtree(temp_dir_path)

    except:
        print('The following temporary directory will be created: ', temp_dir_path)
        pass
    # create folder a temporary folder titled (titled 'tempFolder'
    os.makedirs(temp_dir_path)

    return temp_dir_path, final_user


def temp_dir_folders_fn(temp_dir_path):
    """ Create folders within the temp_dir directory.

    @param temp_dir_path: string object containing the newly created directory path.
    @return prime_temp_grid_dir: string object containing the newly created folder (temp_tile_grid) within the
    temporary directory.
    @return prime_temp_buffer_dir: string object containing the newly created folder (temp_1ha_buffer)within the
    temporary directory.

    """

    prime_temp_grid_dir = temp_dir_path + '\\temp_tile_grid'
    os.mkdir(prime_temp_grid_dir)

    zonal_stats_ready_dir = prime_temp_grid_dir + '\\zonal_stats_ready'
    os.makedirs(zonal_stats_ready_dir)

    proj_tile_grid_sep_dir = prime_temp_grid_dir + '\\separation'
    os.makedirs(proj_tile_grid_sep_dir)

    prime_temp_buffer_dir = temp_dir_path + '\\temp_1ha_buffer'
    os.mkdir(prime_temp_buffer_dir)

    gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    os.mkdir(gcs_wgs84_dir)

    albers_dir = (temp_dir_path + '\\albers')
    os.mkdir(albers_dir)

    return prime_temp_grid_dir, prime_temp_buffer_dir, zonal_stats_ready_dir


def export_file_path_fn(export_dir, final_user, path, row):
    """ Create an export directory 'user_YYYMMDD_HHMM' at the location specified in command argument export_dir.

    @param final_user: string object containing the user id or the operator.
    @param export_dir: string object containing the path to the export directory (command argument).
    @return export_dir_path: string object containing the newly created directory path for all retained exports.
    """

    # create string object from final_user and datetime.
    date_time_replace = str(datetime.now()).replace('-', '')
    date_time_list = date_time_replace.split(' ')
    date_time_list_split = date_time_list[1].split(':')
    export_dir_path = export_dir + '\\' + final_user + '_' + str(path) + '_' + str(row) + '_hdzs_' + str(
        date_time_list[0]) + '_' + str(
        date_time_list_split[0]) + str(
        date_time_list_split[1])

    # check if the folder already exists - if False = create directory, if True = return error message.
    try:
        shutil.rmtree(export_dir_path)

    except:
        print('The following export directory will be created: ', export_dir_path)
        pass

    # create folder.
    os.makedirs(export_dir_path)

    return export_dir_path


def export_dir_folders_fn(export_dir_path, lsat_tile):
    """ Create sub-folders within the export directory.

    @param export_dir_path: string object containing the newly created export directory path.
    @return tile_status_dir: string object containing the newly created folder (tile_status) with three sub-folders:
    for_processing, insufficient_files and tile_status_lists.
    @return tile_status_dir:
    @return plot_dir:
    @return zonal_stats_output_dir:
    @return rainfall_output_dir:
    """

    h99_tile_status_dir = (export_dir_path + '\\h99_tile_status')
    os.mkdir(h99_tile_status_dir)

    hcv_tile_status_dir = (export_dir_path + '\\hcv_tile_status')
    os.mkdir(hcv_tile_status_dir)

    hmc_tile_status_dir = (export_dir_path + '\\hmc_tile_status')
    os.mkdir(hmc_tile_status_dir)

    hsd_tile_status_dir = (export_dir_path + '\\hsd_tile_status')
    os.mkdir(hsd_tile_status_dir)

    fdc_tile_status_dir = (export_dir_path + '\\fdc_tile_status')
    os.mkdir(fdc_tile_status_dir)

    ccw_tile_status_dir = (export_dir_path + '\\ccw_tile_status')
    os.mkdir(ccw_tile_status_dir)

    n17_tile_status_dir = (export_dir_path + '\\n17_tile_status')
    os.mkdir(n17_tile_status_dir)

    wdc_tile_status_dir = (export_dir_path + '\\wdc_tile_status')
    os.mkdir(wdc_tile_status_dir)

    wfp_tile_status_dir = (export_dir_path + '\\wfp_tile_status')
    os.mkdir(wfp_tile_status_dir)
    # ----------------------------------------------------------------------


    h99_tile_for_processing_dir = (h99_tile_status_dir + '\\h99_for_processing')
    os.mkdir(h99_tile_for_processing_dir)

    hcv_tile_for_processing_dir = (hcv_tile_status_dir + '\\hcv_for_processing')
    os.mkdir(hcv_tile_for_processing_dir)

    hmc_tile_for_processing_dir = (hmc_tile_status_dir + '\\hmc_for_processing')
    os.mkdir(hmc_tile_for_processing_dir)

    hsd_tile_for_processing_dir = (hsd_tile_status_dir + '\\hsd_for_processing')
    os.mkdir(hsd_tile_for_processing_dir)

    fdc_tile_for_processing_dir = (fdc_tile_status_dir + '\\fdc_for_processing')
    os.mkdir(fdc_tile_for_processing_dir)

    ccw_tile_for_processing_dir = (ccw_tile_status_dir + '\\ccw_for_processing')
    os.mkdir(ccw_tile_for_processing_dir)

    n17_tile_for_processing_dir = (n17_tile_status_dir + '\\n17_for_processing')
    os.mkdir(n17_tile_for_processing_dir)

    wdc_tile_for_processing_dir = (wdc_tile_status_dir + '\\wdc_for_processing')
    os.mkdir(wdc_tile_for_processing_dir)

    wfp_tile_for_processing_dir = (wfp_tile_status_dir + '\\wfp_for_processing')
    os.mkdir(wfp_tile_for_processing_dir)
    # # -----------------------------------------------------------------------


    h99_insuf_files_dir = (h99_tile_status_dir + '\\h99_insufficient_files')
    os.mkdir(h99_insuf_files_dir)

    hcv_insuf_files_dir = (hcv_tile_status_dir + '\\hcv_insufficient_files')
    os.mkdir(hcv_insuf_files_dir)

    hmc_insuf_files_dir = (hmc_tile_status_dir + '\\hmc_insufficient_files')
    os.mkdir(hmc_insuf_files_dir)
    # # ------------------------------------------------------------------------

    h99_stat_list_dir = h99_tile_status_dir + '\\h99_tile_status_lists'
    os.mkdir(h99_stat_list_dir)

    hcv_stat_list_dir = hcv_tile_status_dir + '\\hcv_tile_status_lists'
    os.mkdir(hcv_stat_list_dir)

    hmc_stat_list_dir = hmc_tile_status_dir + '\\hmc_tile_status_lists'
    os.mkdir(hmc_stat_list_dir)

    hsd_stat_list_dir = hsd_tile_status_dir + '\\hsd_tile_status_lists'
    os.mkdir(hsd_stat_list_dir)

    fdc_stat_list_dir = fdc_tile_status_dir + '\\fdc_tile_status_lists'
    os.mkdir(fdc_stat_list_dir)

    ccw_stat_list_dir = ccw_tile_status_dir + '\\ccw_tile_status_lists'
    os.mkdir(ccw_stat_list_dir)

    n17_stat_list_dir = n17_tile_status_dir + '\\n17_tile_status_lists'
    os.mkdir(n17_stat_list_dir)

    wdc_stat_list_dir = wdc_tile_status_dir + '\\wdc_tile_status_lists'
    os.mkdir(wdc_stat_list_dir)

    wfp_stat_list_dir = wfp_tile_status_dir + '\\wfp_tile_status_lists'
    os.mkdir(wfp_stat_list_dir)

    # -------------------------------------------------------------------------



    h99_zonal_stats_output_dir = (export_dir_path + '\\h99_zonal_stats')
    os.mkdir(h99_zonal_stats_output_dir)

    hcv_zonal_stats_output_dir = (export_dir_path + '\\hcv_zonal_stats')
    os.mkdir(hcv_zonal_stats_output_dir)

    hmc_zonal_stats_output_dir = (export_dir_path + '\\hmc_zonal_stats')
    os.mkdir(hmc_zonal_stats_output_dir)

    hsd_zonal_stats_output_dir = (export_dir_path + '\\hsd_zonal_stats')
    os.mkdir(hsd_zonal_stats_output_dir)

    fdc_zonal_stats_output_dir = (export_dir_path + '\\fdc_zonal_stats')
    os.mkdir(fdc_zonal_stats_output_dir)

    ccw_zonal_stats_output_dir = (export_dir_path + '\\ccw_zonal_stats')
    os.mkdir(ccw_zonal_stats_output_dir)

    n17_zonal_stats_output_dir = (export_dir_path + '\\n17_zonal_stats')
    os.mkdir(n17_zonal_stats_output_dir)

    wdc_zonal_stats_output_dir = (export_dir_path + '\\wdc_zonal_stats')
    os.mkdir(wdc_zonal_stats_output_dir)

    wfp_zonal_stats_output_dir = (export_dir_path + '\\wfp_zonal_stats')
    os.mkdir(wfp_zonal_stats_output_dir)

    return h99_tile_status_dir, hcv_tile_status_dir, hmc_tile_status_dir, hsd_tile_status_dir, fdc_tile_status_dir, \
        ccw_tile_status_dir, n17_tile_status_dir, wdc_tile_status_dir, wfp_tile_status_dir


def main_routine():
    """" Description: This script determines which Landsat tile had the most non null zonal statistics records per site
    and files those plots (bare ground, all bands and interactive) into final output folders. """

    # print('fcZonalStatsPipeline.py INITIATED.')
    # read in the command arguments
    cmd_args = get_cmd_args_fn()
    data = cmd_args.data
    tile_grid = cmd_args.tile_grid
    export_dir = cmd_args.export_dir
    lsat_dir = cmd_args.lsat_dir
    no_data = int(cmd_args.no_data)
    path = cmd_args.path
    row = cmd_args.row
    zone = cmd_args.zone
    image_count = int(cmd_args.image_count)

    # call the temporaryDir function.
    temp_dir_path, final_user = temporary_dir_fn()
    # call the tempDirFolders function.
    prime_temp_grid_dir, prime_temp_buffer_dir, zonal_stats_ready_dir = temp_dir_folders_fn(temp_dir_path)
    # call the exportFilepath function.
    export_dir_path = export_file_path_fn(export_dir, final_user, path, row)
    print("zonal_stats_ready_dir: ", zonal_stats_ready_dir)
    # # create a list of variable subdirectories
    # sub_dir_list = next(os.walk(lsat_dir))[1]

    lsat_tile = str(path) + "_" + str(row)
    #
    # call the exportDirFolders function.

    h99_tile_status_dir, hcv_tile_status_dir, hmc_tile_status_dir, hsd_tile_status_dir, fdc_tile_status_dir, \
        ccw_tile_status_dir, n17_tile_status_dir, wdc_tile_status_dir, wfp_tile_status_dir = export_dir_folders_fn(export_dir_path, lsat_tile)


    # export_dir_folders_fn(export_dir_path, lsat_tile)

    print("data: ", data)
    import step1_3_project_buffer
    geo_df2, crs_name = step1_3_project_buffer.main_routine(data, zone, export_dir_path, prime_temp_buffer_dir)

    import step1_4_landsat_tile_grid_identify2
    comp_geo_df, zonal_stats_ready_dir = step1_4_landsat_tile_grid_identify2.main_routine(
        tile_grid, geo_df2, data, zone, export_dir_path, prime_temp_grid_dir)

    print("zonal_stats_ready_dir: ", zonal_stats_ready_dir)
    comp_geo_df.to_file(os.path.join(export_dir_path, "biomass_1ha.shp"))

    tile = str(path) + str(row)
    geo_df3 = comp_geo_df[comp_geo_df["tile"] == tile]
    geo_df4 = geo_df3[["site_name", "tile", "geometry"]]
    print("geodf4: ", geo_df4)

    geo_df4.reset_index(drop=True, inplace=True)
    geo_df4['uid'] = geo_df4.index + 1

    shapefile_path = os.path.join(export_dir_path, "biomass_1ha_{0}.shp".format(str(path) + "_" + str(row)))
    geo_df4.to_file(os.path.join(export_dir_path, "biomass_1ha_{0}.shp".format(str(path) + "_" + str(row))),
                    driver="ESRI Shapefile")

    print("Exported shapefile: ", shapefile_path)

    # ------------------------------------------- H99 ----------------------------------------------------------

    extension = "h99"
    no_data = 0.0

    print("h99_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_h99_landsat_list
    step1_5_h99_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("h99_tile_status_dir: ", h99_tile_status_dir)
    # define the tile for processing directory.
    h99_tile_for_processing_dir = (h99_tile_status_dir + '\\h99_for_processing')
    print('-' * 50)

    h99_zonal_stats_output = (export_dir_path + '\\h99_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    h99_list_zonal_tile = []

    for file in glob.glob(h99_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        h99_list_zonal_tile.append(file)

    print("-" * 50)
    print("h99: ", h99_list_zonal_tile)

    if len(h99_list_zonal_tile) >= 1:
        #
        for csv_file in h99_list_zonal_tile:
            print("h99_zonal_stats_output: ", h99_zonal_stats_output)
            # call the step1_6_h99_zonal_stats.py script.
            import step1_6_h99_zonal_stats_v2
            h99_output_zonal_stats, h99_complete_tile, h99_tile, h99_temp_dir_bands = step1_6_h99_zonal_stats_v2.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, h99_zonal_stats_output, shapefile_path, "h99")


    else:
        print("No h99 images were located")

    # ------------------------------------------- Hcv ----------------------------------------------------------

    extension = "hcv"
    no_data = 0.0

    print("hcv_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_hcv_landsat_list
    step1_5_hcv_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("hcv_tile_status_dir: ", hcv_tile_status_dir)
    # define the tile for processing directory.
    hcv_tile_for_processing_dir = (hcv_tile_status_dir + '\\hcv_for_processing')
    print('-' * 50)

    hcv_zonal_stats_output = (export_dir_path + '\\hcv_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    hcv_list_zonal_tile = []

    for file in glob.glob(hcv_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        hcv_list_zonal_tile.append(file)

    print("-" * 50)
    print("hcv: ", hcv_list_zonal_tile)

    if len(hcv_list_zonal_tile) >= 1:
        #
        for csv_file in hcv_list_zonal_tile:
            print("hcv_zonal_stats_output: ", hcv_zonal_stats_output)
            # call the step1_6_hcv_zonal_stats.py script.
            import step1_6_hcv_zonal_stats_v2
            hcv_output_zonal_stats, hcv_complete_tile, hcv_tile, hcv_temp_dir_bands = step1_6_hcv_zonal_stats_v2.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, hcv_zonal_stats_output, shapefile_path, "hcv")


    else:
        print("No hcv images were located")


    # ------------------------------------------- Hmc ----------------------------------------------------------

    extension = "hmc"
    no_data = 0.0

    print("hmc_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_hmc_landsat_list
    step1_5_hmc_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("hmc_tile_status_dir: ", hmc_tile_status_dir)
    # define the tile for processing directory.
    hmc_tile_for_processing_dir = (hmc_tile_status_dir + '\\hmc_for_processing')
    print('-' * 50)

    hmc_zonal_stats_output = (export_dir_path + '\\hmc_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    hmc_list_zonal_tile = []

    for file in glob.glob(hmc_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        hmc_list_zonal_tile.append(file)

    print("-" * 50)
    print("hmc: ", hmc_list_zonal_tile)

    if len(hmc_list_zonal_tile) >= 1:
        #
        for csv_file in hmc_list_zonal_tile:
            print("hmc_zonal_stats_output: ", hmc_zonal_stats_output)
            # call the step1_6_hmc_zonal_stats.py script.
            import step1_6_hmc_zonal_stats_v2
            hmc_output_zonal_stats, hmc_complete_tile, hmc_tile, hmc_temp_dir_bands = step1_6_hmc_zonal_stats_v2.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, hmc_zonal_stats_output, shapefile_path, "hmc")


    else:
        print("No hmc images were located")


    # ------------------------------------------- hsd ----------------------------------------------------------

    extension = "hsd"
    no_data = 0.0

    print("hsd_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_hsd_landsat_list
    step1_5_hsd_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("hsd_tile_status_dir: ", hsd_tile_status_dir)
    # define the tile for processing directory.
    hsd_tile_for_processing_dir = (hsd_tile_status_dir + '\\hsd_for_processing')
    print('-' * 50)

    hsd_zonal_stats_output = (export_dir_path + '\\hsd_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    hsd_list_zonal_tile = []

    for file in glob.glob(hsd_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        hsd_list_zonal_tile.append(file)

    print("-" * 50)
    print("hsd: ", hsd_list_zonal_tile)

    if len(hsd_list_zonal_tile) >= 1:
        #
        for csv_file in hsd_list_zonal_tile:
            print("hsd_zonal_stats_output: ", hsd_zonal_stats_output)
            # call the step1_6_hsd_zonal_stats.py script.
            import step1_6_hsd_zonal_stats_v2
            hsd_output_zonal_stats, hsd_complete_tile, hsd_tile, hsd_temp_dir_bands = step1_6_hsd_zonal_stats_v2.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, hsd_zonal_stats_output, shapefile_path, "hsd")


    else:
        print("No hsd images were located")



    # # ------------------------------------------- fdc ----------------------------------------------------------

    extension = "fdc"
    no_data = 0.0

    print("fdc_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_fdc_landsat_list
    step1_5_fdc_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("fdc_tile_status_dir: ", fdc_tile_status_dir)
    # define the tile for processing directory.
    fdc_tile_for_processing_dir = (fdc_tile_status_dir + '\\fdc_for_processing')
    print('-' * 50)

    fdc_zonal_stats_output = (export_dir_path + '\\fdc_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    fdc_list_zonal_tile = []

    for file in glob.glob(fdc_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        fdc_list_zonal_tile.append(file)

    print("-" * 50)
    print("fdc: ", fdc_list_zonal_tile)

    if len(fdc_list_zonal_tile) >= 1:
        #
        for csv_file in fdc_list_zonal_tile:
            print("fdc_zonal_stats_output: ", fdc_zonal_stats_output)
            # call the step1_6_fdc_zonal_stats.py script.
            import step1_6_fdc_zonal_stats_v4
            fdc_output_zonal_stats, fdc_complete_tile, fdc_tile, fdc_temp_dir_bands = step1_6_fdc_zonal_stats_v4.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, fdc_zonal_stats_output, shapefile_path, "fdc")


    else:
        print("No fdc images were located")




    # ------------------------------------------- wdc ----------------------------------------------------------

    extension = "wdc"
    no_data = 0.0

    print("wdc_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_wdc_landsat_list
    step1_5_wdc_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("wdc_tile_status_dir: ", wdc_tile_status_dir)
    # define the tile for processing directory.
    wdc_tile_for_processing_dir = (wdc_tile_status_dir + '\\wdc_for_processing')
    print('-' * 50)

    wdc_zonal_stats_output = (export_dir_path + '\\wdc_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    wdc_list_zonal_tile = []

    for file in glob.glob(wdc_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        wdc_list_zonal_tile.append(file)

    print("-" * 50)
    print("wdc: ", wdc_list_zonal_tile)

    if len(wdc_list_zonal_tile) >= 1:
        #
        for csv_file in wdc_list_zonal_tile:
            print("wdc_zonal_stats_output: ", wdc_zonal_stats_output)
            # call the step1_6_wdc_zonal_stats.py script.
            import step1_6_wdc_zonal_stats_v4
            wdc_output_zonal_stats, wdc_complete_tile, wdc_tile, wdc_temp_dir_bands = step1_6_wdc_zonal_stats_v4.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, wdc_zonal_stats_output, shapefile_path, "wdc")


    else:
        print("No wdc images were located")


    # ------------------------------------------- ccw ----------------------------------------------------------

    extension = "ccw"
    no_data = 0.0

    print("ccw_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_ccw_landsat_list
    step1_5_ccw_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("ccw_tile_status_dir: ", ccw_tile_status_dir)
    # define the tile for processing directory.
    ccw_tile_for_processing_dir = (ccw_tile_status_dir + '\\ccw_for_processing')
    print('-' * 50)

    ccw_zonal_stats_output = (export_dir_path + '\\ccw_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    ccw_list_zonal_tile = []

    for file in glob.glob(ccw_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        ccw_list_zonal_tile.append(file)

    print("-" * 50)
    print("ccw: ", ccw_list_zonal_tile)

    if len(ccw_list_zonal_tile) >= 1:
        #
        for csv_file in ccw_list_zonal_tile:
            print("ccw_zonal_stats_output: ", ccw_zonal_stats_output)
            # call the step1_6_ccw_zonal_stats.py script.
            import step1_6_ccw_zonal_stats_v2
            ccw_output_zonal_stats, ccw_complete_tile, ccw_tile, ccw_temp_dir_bands = step1_6_ccw_zonal_stats_v2.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, ccw_zonal_stats_output, shapefile_path, "ccw")


    else:
        print("No fdc images were located")

        # ------------------------------------------- n17 ----------------------------------------------------------

    extension = "n17"
    no_data = 0.0

    print("n17_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_n17_landsat_list
    step1_5_n17_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("n17_tile_status_dir: ", n17_tile_status_dir)
    # define the tile for processing directory.
    n17_tile_for_processing_dir = (n17_tile_status_dir + '\\n17_for_processing')
    print('-' * 50)

    n17_zonal_stats_output = (export_dir_path + '\\n17_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    n17_list_zonal_tile = []

    for file in glob.glob(n17_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        n17_list_zonal_tile.append(file)

    print("-" * 50)
    print("n17: ", n17_list_zonal_tile)

    if len(n17_list_zonal_tile) >= 1:
        #
        for csv_file in n17_list_zonal_tile:
            print("n17_zonal_stats_output: ", n17_zonal_stats_output)
            # call the step1_6_n17_zonal_stats.py script.
            import step1_6_n17_zonal_stats_v4
            n17_output_zonal_stats, n17_complete_tile, n17_tile, n17_temp_dir_bands = step1_6_n17_zonal_stats_v4.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, n17_zonal_stats_output, shapefile_path,
                "n17")


    else:
        print("No n17 images were located")


    # ------------------------------------------- wfp ----------------------------------------------------------

    extension = "wfp"
    no_data = 0.0

    print("wfp_" * 50)

    # call the step1_5_dil_landsat_list.py script.
    import step1_5_wfp_landsat_list
    step1_5_wfp_landsat_list.main_routine(
        export_dir_path, geo_df3, image_count, lsat_dir, path, row, zone, extension)

    print("up to here")
    print("wfp_tile_status_dir: ", wfp_tile_status_dir)
    # define the tile for processing directory.
    wfp_tile_for_processing_dir = (wfp_tile_status_dir + '\\wfp_for_processing')
    print('-' * 50)

    wfp_zonal_stats_output = (export_dir_path + '\\wfp_zonal_stats')
    # print('dil zonal_stats_output: ', dil_zonal_stats_output)
    wfp_list_zonal_tile = []

    for file in glob.glob(wfp_tile_for_processing_dir + '\\*.csv'):
        print(file)
        # append tile paths to list.
        wfp_list_zonal_tile.append(file)

    print("-" * 50)
    print("wfp: ", wfp_list_zonal_tile)

    if len(wfp_list_zonal_tile) >= 1:
        #
        for csv_file in wfp_list_zonal_tile:
            print("wfp_zonal_stats_output: ", wfp_zonal_stats_output)
            # call the step1_6_wfp_zonal_stats.py script.
            import step1_6_wfp_zonal_stats_v2
            wfp_output_zonal_stats, wfp_complete_tile, wfp_tile, wfp_temp_dir_bands = step1_6_wfp_zonal_stats_v2.main_routine(
                temp_dir_path, zonal_stats_ready_dir, no_data, csv_file, wfp_zonal_stats_output, shapefile_path, "wfp")


    else:
        print("No fdc images were located")

    # ---------------------------------------------------- Clean up ----------------------------------------------------

    shutil.rmtree(temp_dir_path)
    print('Temporary directory and its contents has been deleted from your working drive.')
    print(' - ', temp_dir_path)
    print('fractional cover zonal stats pipeline is complete.')
    print('goodbye.')


if __name__ == '__main__':
    main_routine()
