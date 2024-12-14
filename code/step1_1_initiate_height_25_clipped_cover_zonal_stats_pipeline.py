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
import geopandas as gpd
import csv

warnings.filterwarnings("ignore")


def get_cmd_args_fn():
    p = argparse.ArgumentParser(
        description='''Input a single or multi-band raster to extracts the values from the input shapefile. ''')

    p.add_argument('-d', '--data', help='The directory the site points csv file.')

    p.add_argument('-t', '--tile_grid',
                   help="Enter filepath for the Landsat Tile Grid.shp.",
                   default=r"C:\Users\robot\code\pipelines\tile_biomass_zonal_height_density_pipeline\assets\shapefiles\Landsat_wrs2_TileGrid.shp")

    p.add_argument('-x', '--export_dir',
                   help='Enter the export directory for all of the final outputs.',
                   default=r'U:\scratch\rob\pipelines\outputs')

    p.add_argument('-i', '--image_count', type=int,
                   help='Enter the minimum amount of Landsat images required per tile as an integer (i.e. 950).',
                   default=10)

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

    h25_tile_status_dir = (export_dir_path + '\\h25_tile_status')
    os.mkdir(h25_tile_status_dir)

    # hcv_tile_status_dir = (export_dir_path + '\\hcv_tile_status')
    # os.mkdir(hcv_tile_status_dir)
    #
    # hmc_tile_status_dir = (export_dir_path + '\\hmc_tile_status')
    # os.mkdir(hmc_tile_status_dir)
    #
    # hsd_tile_status_dir = (export_dir_path + '\\hsd_tile_status')
    # os.mkdir(hsd_tile_status_dir)
    #
    # fdc_tile_status_dir = (export_dir_path + '\\fdc_tile_status')
    # os.mkdir(fdc_tile_status_dir)
    #
    # ccw_tile_status_dir = (export_dir_path + '\\ccw_tile_status')
    # os.mkdir(ccw_tile_status_dir)
    #
    # n17_tile_status_dir = (export_dir_path + '\\n17_tile_status')
    # os.mkdir(n17_tile_status_dir)
    #
    # wdc_tile_status_dir = (export_dir_path + '\\wdc_tile_status')
    # os.mkdir(wdc_tile_status_dir)
    #
    # wfp_tile_status_dir = (export_dir_path + '\\wfp_tile_status')
    # os.mkdir(wfp_tile_status_dir)
    # ----------------------------------------------------------------------


    h25_tile_for_processing_dir = (h25_tile_status_dir + '\\h25_for_processing')
    os.mkdir(h25_tile_for_processing_dir)

    # hcv_tile_for_processing_dir = (hcv_tile_status_dir + '\\hcv_for_processing')
    # os.mkdir(hcv_tile_for_processing_dir)
    #
    # hmc_tile_for_processing_dir = (hmc_tile_status_dir + '\\hmc_for_processing')
    # os.mkdir(hmc_tile_for_processing_dir)
    #
    # hsd_tile_for_processing_dir = (hsd_tile_status_dir + '\\hsd_for_processing')
    # os.mkdir(hsd_tile_for_processing_dir)
    #
    # fdc_tile_for_processing_dir = (fdc_tile_status_dir + '\\fdc_for_processing')
    # os.mkdir(fdc_tile_for_processing_dir)
    #
    # ccw_tile_for_processing_dir = (ccw_tile_status_dir + '\\ccw_for_processing')
    # os.mkdir(ccw_tile_for_processing_dir)
    #
    # n17_tile_for_processing_dir = (n17_tile_status_dir + '\\n17_for_processing')
    # os.mkdir(n17_tile_for_processing_dir)
    #
    # wdc_tile_for_processing_dir = (wdc_tile_status_dir + '\\wdc_for_processing')
    # os.mkdir(wdc_tile_for_processing_dir)
    #
    # wfp_tile_for_processing_dir = (wfp_tile_status_dir + '\\wfp_for_processing')
    # os.mkdir(wfp_tile_for_processing_dir)
    # # -----------------------------------------------------------------------


    h25_insuf_files_dir = (h25_tile_status_dir + '\\h25_insufficient_files')
    os.mkdir(h25_insuf_files_dir)

    # hcv_insuf_files_dir = (hcv_tile_status_dir + '\\hcv_insufficient_files')
    # os.mkdir(hcv_insuf_files_dir)
    #
    # hmc_insuf_files_dir = (hmc_tile_status_dir + '\\hmc_insufficient_files')
    # os.mkdir(hmc_insuf_files_dir)
    # # ------------------------------------------------------------------------

    h25_stat_list_dir = h25_tile_status_dir + '\\h25_tile_status_lists'
    os.mkdir(h25_stat_list_dir)
    #
    # hcv_stat_list_dir = hcv_tile_status_dir + '\\hcv_tile_status_lists'
    # os.mkdir(hcv_stat_list_dir)
    #
    # hmc_stat_list_dir = hmc_tile_status_dir + '\\hmc_tile_status_lists'
    # os.mkdir(hmc_stat_list_dir)
    #
    # hsd_stat_list_dir = hsd_tile_status_dir + '\\hsd_tile_status_lists'
    # os.mkdir(hsd_stat_list_dir)
    #
    # fdc_stat_list_dir = fdc_tile_status_dir + '\\fdc_tile_status_lists'
    # os.mkdir(fdc_stat_list_dir)
    #
    # ccw_stat_list_dir = ccw_tile_status_dir + '\\ccw_tile_status_lists'
    # os.mkdir(ccw_stat_list_dir)
    #
    # n17_stat_list_dir = n17_tile_status_dir + '\\n17_tile_status_lists'
    # os.mkdir(n17_stat_list_dir)
    #
    # wdc_stat_list_dir = wdc_tile_status_dir + '\\wdc_tile_status_lists'
    # os.mkdir(wdc_stat_list_dir)
    #
    # wfp_stat_list_dir = wfp_tile_status_dir + '\\wfp_tile_status_lists'
    # os.mkdir(wfp_stat_list_dir)

    # -------------------------------------------------------------------------



    h25_zonal_stats_output_dir = (export_dir_path + '\\h25_zonal_stats')
    os.mkdir(h25_zonal_stats_output_dir)

    # hcv_zonal_stats_output_dir = (export_dir_path + '\\hcv_zonal_stats')
    # os.mkdir(hcv_zonal_stats_output_dir)
    #
    # hmc_zonal_stats_output_dir = (export_dir_path + '\\hmc_zonal_stats')
    # os.mkdir(hmc_zonal_stats_output_dir)
    #
    # hsd_zonal_stats_output_dir = (export_dir_path + '\\hsd_zonal_stats')
    # os.mkdir(hsd_zonal_stats_output_dir)
    #
    # fdc_zonal_stats_output_dir = (export_dir_path + '\\fdc_zonal_stats')
    # os.mkdir(fdc_zonal_stats_output_dir)
    #
    # ccw_zonal_stats_output_dir = (export_dir_path + '\\ccw_zonal_stats')
    # os.mkdir(ccw_zonal_stats_output_dir)
    #
    # n17_zonal_stats_output_dir = (export_dir_path + '\\n17_zonal_stats')
    # os.mkdir(n17_zonal_stats_output_dir)
    #
    # wdc_zonal_stats_output_dir = (export_dir_path + '\\wdc_zonal_stats')
    # os.mkdir(wdc_zonal_stats_output_dir)
    #
    # wfp_zonal_stats_output_dir = (export_dir_path + '\\wfp_zonal_stats')
    # os.mkdir(wfp_zonal_stats_output_dir)

    return h25_tile_status_dir

def check_unique_values(tile_list, lsat_list):

    #print(tile_list, lsat_list)

    # import sys
    # sys.exit()
    # Convert the list to a set to remove duplicates
    unique_values = set(tile_list)

    # Create a dictionary to store lists of separated strings
    separated_lists = {substring: [] for substring in unique_values}

    # Check the number of unique values
    if len(unique_values) == 1:
        print("The list contains a single unique value.")
        # Assign the entire list to the single key in separated_lists
        single_value = list(unique_values)[0]
        separated_lists[single_value] = lsat_list
    else:
        print("The list contains more than one unique value.")
        # Iterate through the lsat_list and separate based on substring
        for string in lsat_list:
            # Check which unique substring is in the current string
            for substring in unique_values:
                # Convert '103_071' to 'p103r071' for matching
                formatted_substring = f"p{substring.replace('_', 'r')}"
                if formatted_substring in string:
                    separated_lists[substring].append(string)
                    break  # No need to check further substrings once matched

    # Print the separated lists
    for substring, separated_list in separated_lists.items():
        print(f"Strings containing '{substring}': {separated_list}")

    return separated_lists, unique_values

def find_shp_files(starting_directory, prefix):
    shp_files = []
    for root, dirs, files in os.walk(starting_directory):
        for file in files:
            if file.endswith('.shp') and file.startswith(prefix):
                shp_files.append(os.path.join(root, file))
    return shp_files


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
    #zone = cmd_args.zone
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

    #lsat_tile = str(path) + "_" + str(row)
    #
    # call the exportDirFolders function.

    #h25_tile_status_dir = export_dir_folders_fn(export_dir_path, lsat_tile)


    # export_dir_folders_fn(export_dir_path, lsat_tile)

    zones_dir = r"H:\height25\shp\zones"
    #zones_dir = os.listdir(r"H:\height25\shp\zones")
    #print(zones_dir)

    # ------------------------------------- No fire mask img ------------------------------
    print("-"*100)
    print("dbi")
    directory_path = r'H:\height25\dbi'
    #print("Looking in : ", directory_path)
    list_dir = os.listdir(directory_path)
    sorted_list_dir = sorted(list_dir)
    print("sorted_list_dir: ", sorted_list_dir)

    # import sys
    # sys.exit()

    for site in sorted_list_dir:
        print("=" * 100)
        print("site: ", site)
        path_ = os.path.join(directory_path, site)
        list_dir = os.listdir(path_)

        print("list_dir: ", list_dir)
        # Filter to get only files ending with '.img' and exclude '.img.aux.xml'
        # Filter to get only files ending with '.img' and exclude '.img.aux' or '.img.aux.xml'
        img_files = [file for file in list_dir if
                     file.endswith('.img') and not (file.endswith('.img.aux.xml') or file.endswith('.img.aux'))]

        if len(img_files) > 0:
            z = img_files[0][-5:-4]
            print("img_files: ", img_files[0])
            print("z: ", z)
            if z == '2':
                zone_dir = os.path.join(zones_dir, "z52752")
            elif z == '3':
                zone_dir = os.path.join(zones_dir, "z52753")
            elif z == '4':
                zone_dir = os.path.join(zones_dir, "z52754")

            shp_files = find_shp_files(zone_dir, site)

            if len(shp_files) > 0:
                shp = shp_files[0]
                ex_dir = os.path.join(export_dir, site)
                if not os.path.isdir(ex_dir):
                    os.mkdir(ex_dir)

                lsat_tile_list = []
                lsat_list = []
                for img in glob.glob(os.path.join(path_, "*h25m?.img")):
                    print("img: ", img)
                    lsat_list.append(img)
                    img_split = img.split("_")
                    path = img_split[1][1:4]
                    row = img_split[1][5:]
                    lsat_tile_list.append(f"{img_split[1][1:4]}_{img_split[1][5:]}")
                    site_status_dir = os.path.join(zonal_stats_ready_dir, site)

                separated_list, unique_values = check_unique_values(lsat_tile_list, lsat_list)
                print("seperated_list: ", separated_list)
                gdf = gpd.read_file(shp)
                gdf.reset_index(drop=True, inplace=True)
                gdf['uid'] = gdf.index + 1
                gdf['site'] = site

                for lsat_tile, file_list in separated_list.items():

                    lsat_tile = next(iter(unique_values))
                    gdf['lsat'] = lsat_tile
                    shp_path = os.path.join(temp_dir_path, f"{site}_{lsat_tile}.shp")
                    gdf.to_file(shp_path)

                    csv_output = os.path.join(temp_dir_path,
                                              f'Complete_list_of_{site}_h25_tiles_ready_for_zonal_stats.csv')
                    with open(csv_output, "w") as output:
                        writer = csv.writer(output, lineterminator='\n')
                        for file in lsat_list:
                            writer.writerow([file])

                    import step1_6_h25_zonal_stats
                    step1_6_h25_zonal_stats.main_routine(temp_dir_path, no_data, lsat_tile, ex_dir,
                                                         shp_path, "h25", csv_output)

            # If shp_files found and processed, continue to next site
            continue
        else:
            print("SITE not located: ", site)
            # Continue to next site after logging the error
            continue

    # ------------------------------------- fire mask img ------------------------------
    print("-"*100)
    print("dbi fire mask")
    directory_path = r'H:\height25\dbi_dknmask'
    #print("Looking in : ", directory_path)

    list_dir = os.listdir(directory_path)
    sorted_list_dir = sorted(list_dir)
    for site in sorted_list_dir:
        print("site: ", site)
        path_ = os.path.join(directory_path, site)
        list_dir = os.listdir(path_)

        # Filter to get only files ending with '.img' and exclude '.img.aux' or '.img.aux.xml'
        img_files = [file for file in list_dir if
                     file.endswith('.img') and not (file.endswith('.img.aux.xml') or file.endswith('.img.aux'))]


        if len(img_files) > 0:
            z = img_files[0][-14:-13]
            print("len dir: ", img_files[0])
            print("z: ", z)
            if z == '2':
                zone_dir = os.path.join(zones_dir, "z52752")
            elif z == '3':
                zone_dir = os.path.join(zones_dir, "z52753")
            elif z == '4':
                zone_dir = os.path.join(zones_dir, "z52754")

            print("shp from dir: ", zone_dir)
            shp_files = find_shp_files(zone_dir, site)

            if len(shp_files) > 0:
                shp = shp_files[0]
                ex_dir = os.path.join(export_dir, site)
                if not os.path.isdir(ex_dir):
                    os.mkdir(ex_dir)

                lsat_tile_list = []
                lsat_list = []

                for img in glob.glob(os.path.join(path_, "*.img")):
                    lsat_list.append(img)
                    img_split = img.split("_")
                    path = img_split[2][1:4]
                    row = img_split[2][5:]
                    lsat_tile_list.append(f"{img_split[2][1:4]}_{img_split[2][5:]}")
                    site_status_dir = os.path.join(zonal_stats_ready_dir, site)

                separated_list, unique_values = check_unique_values(lsat_tile_list, lsat_list)

                gdf = gpd.read_file(shp)
                gdf.reset_index(drop=True, inplace=True)
                gdf['uid'] = gdf.index + 1
                gdf['site'] = site

                # Loop through each key-value pair in the dictionary
                for lsat_tile, file_list in separated_list.items():

                    lsat_tile = next(iter(unique_values))
                    gdf['lsat'] = lsat_tile
                    shp_path = os.path.join(temp_dir_path, f"{site}_{lsat_tile}.shp")
                    gdf.to_file(shp_path)

                    csv_output = os.path.join(temp_dir_path,
                                              f'Complete_list_of_{site}_h25_tiles_ready_for_zonal_stats.csv')

                    with open(csv_output, "w") as output:
                        writer = csv.writer(output, lineterminator='\n')
                        for file in lsat_list:
                            writer.writerow([file])

                    # Call external script or function for further processing
                    import step1_6_h25_zonal_stats_mask

                    step1_6_h25_zonal_stats_mask.main_routine(temp_dir_path, no_data, lsat_tile, ex_dir,
                                                              shp_path, "h25", csv_output)

            # Ensure moving to the next site after processing is done
            continue

        else:
            print("SITE not located: ", site)
            # If no shape files found, continue to the next site
            continue


# ---------------------------------------------------- Clean up ----------------------------------------------------

#shutil.rmtree(temp_dir_path)
print('Temporary directory and its contents has been deleted from your working drive.')
#print(' - ', temp_dir_path)
print('fractional cover zonal stats pipeline is complete.')
print('goodbye.')


if __name__ == '__main__':
    main_routine()
