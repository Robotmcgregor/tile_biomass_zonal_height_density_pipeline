#!/usr/bin/env python

"""
step1_5_dp1_landsat_list.py
================
Description: This script searches through each Landsat tile directory that was identified as overlaying with an odk 1hs
site and determines if there are sufficient images for zonal stats processing (greater than fc_count).
If an identified tile contains sufficient  images, each image path will be input into a csv (1 path per line) and the
csv will be saved in the for processing subdirectory. If there are insufficient images then the tile name will be saved
in a csv titled insufficient files saved in the tile status directory of the export directory.


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

========================================================================================================
"""

# Import modules
from __future__ import print_function, division
import os
import csv
import sys
from glob import glob
import warnings

warnings.filterwarnings("ignore")


def append_geo_df_fn(geo_df2, zone, export_dir_path):
    """ Concatenate previously separated projected 1ha sites to a single geo-DataFrame and re-project to
    geographics GDA94.

    @param comp_geo_df_52: geo-dataframe containing 1ha sites with property, site and Landsat tile information projected
    to WGSz52.
    @param comp_geo_df_53: geo-dataframe containing 1ha sites with property, site and Landsat tile information projected
    to WGSz53.
    @param export_dir_path: string object containing the path to the export directory.
    @return geo_df: geo-dataframe containing all inputs projected in GDA94 geographics.
    """
    # Add a feature: crs, to each projected geoDataFrame and fill with a projection string variable.

    if str(zone) == "2":

        geo_df2['crs'] = 'WGSz52'
        print("comp_geo_df_52: ", geo_df2)

    elif str(zone) == "3":
        geo_df2['crs'] = 'WGSz53'
        print("comp_geo_df_53: ", geo_df2)

    elif str(zone) == "4":
        geo_df2['crs'] = 'WGSz54'
        print("comp_geo_df_54: ", geo_df2)

    else:
        print("Script abort unknown zone step 1_5")
        import sys
        sys.exit()

    # Project both geoDataFrames to geographic GDA94.
    geo_df_gda94 = geo_df2.to_crs(epsg=4283)
    # geo_df_gda942 = comp_geo_df_53.to_crs(epsg=4283)

    # Append/concatenate both geoDataFrames into one.
    # geo_df = geo_df_gda941.append(geo_df_gda942)

    # Export geoDataFrame to the export directory (command argument).
    geo_df_gda94.to_file(driver='ESRI Shapefile',
                         filename=export_dir_path + '\\' + 'landsat_tile_site_identity_gda94.shp')

    return geo_df_gda94


def unique_values_fn(geo_df):
    """ Create a list of unique Landsat tiles that overlay with a 1ha site - name restructured from geo-dataframe.

    @param geo_df: geo-dataframe containing all inputs projected in GDA94 geographics.
    @return list_tile_unique: list object containing restructured landsat tiles as a unique list.
    """
    # Create an empty list to store unique manipulated ( i.e. 101077 > 101_077) Landsat tile names.
    list_tile_unique = []

    # Create and fill a list of unique Landsat tile names from the geo_df geoDataFrame feature: TITLE.
    # listTile = (geo_df.TILE.unique()).tolist()
    for landsat_tile in geo_df.tile.unique():
        # String manipulation.
        beginning = str(landsat_tile[:3])
        end = str(landsat_tile[-3:])
        # String concatenation.
        landsat_tile_name = beginning + '_' + end
        # Append concatenated string to empty list titled: list_tile_unique.
        list_tile_unique.append(landsat_tile_name)

    return list_tile_unique


def list_file_directory_fn(landsat_tile_dir, extension, zone):
    """ Create an empty list to store the Landsat image file path for images that meet the search criteria
    (image_search_criteria1 and image_search_criteria2).
    @param landsat_tile_dir:
    @param image_search_criteria1: string object containing the end part of the required file name (--search_criteria1)
    @param image_search_criteria2: string object containing the end part of the required file name (--search_criteria2)
    @return list_landsat_tile_path: list object containing the path to all matching either search criteria.
    """

    print("Landsat tile dir: ", landsat_tile_dir)
    list_landsat_tile_path = []

    # Navigate and loop through the folders within the Landsat Tile Directory stored in the 'landsat_tile_dir'
    # object variable.

    print("extension: ", extension)
    print("{0}m{1}.img".format(extension, str(zone)))
    print("landsat_tile_dir: ", landsat_tile_dir)

    for root, dirs, files in os.walk(landsat_tile_dir):
        for file in files:
            print('file: ', file)
            # Search for files ending with the string value stored in the object variable: imageSearchCriteria.
            if file.endswith("{0}m{1}.img".format(extension, str(zone))):
                # Concatenate the root and file names to create a file path.
                image_path = (os.path.join(root, file))
                #print('LOCATED - image_path: ', image_path)
                # Append the image_path variable to the empty list 'list_landsat_tile_path'.
                list_landsat_tile_path.append(image_path)
    print("list_landsat: ", list_landsat_tile_path)

    return list_landsat_tile_path


def create_csv_list_of_paths_fn(lsat_tile, lsat_dir, extension, image_count, tile_status_dir, path, row, zone):
    """ Determine which Landsat Tiles have a sufficient amount of images to process.

    :param zone:
    :param image_count:
    :param extension:
    :param lsat_dir:
    :param lsat_tile:
    @param tile_status_dir: string object to the subdirectory export_dir\tile_status
    @return list_sufficient: list object containing the path to all Landsat images of interest providing that the
    number was greater than the fc_count value.
    """
    # Crete two empty list to contain Landsat tile names which meet and do not meet the minimum number of images set
    # by the 'fc_count' variable (command argument: fc_count).

    list_insufficient = []
    list_sufficient = []

    # landsat_tile = str(path) + '_' + str(row)

    # Loop through the unique Landsat Tile list ' listTile Unique'.
    landsat_tile_dir = os.path.join(lsat_dir, lsat_tile, "height") #lsat_dir + '\\' + lsat_tile
    print('=' * 50)
    print('Confirm that there are sufficient seasonal fractional cover tiles h99 for processing')
    print('landsat_tile_dir: ', landsat_tile_dir)
    # Run the list_file_directory_fn function.
    list_landsat_tile_path = list_file_directory_fn(landsat_tile_dir, extension, zone)

    print("list_landsat_tile_path: ", list_landsat_tile_path)
    # Calculate the number of image pathways stored in the list_landsat_tile_path variable.
    image_length = (len(list_landsat_tile_path))
    print(' - Total seasonal fractional cover h99 tiles located: ', image_length)

    # Rule set to file Landsat tile names based on the amount of images comparatively to the minimum requirement
    # set by the fc_count variable.
    print(' - Minimum tiles (command argument): ', image_count)
    print('=' * 50)
    if image_length >= image_count:
        # Append landsat_tile to list_sufficient if the number of images are equal to or more that the minimum
        # amount defined in the 'fc_count' variable.
        list_sufficient.append(lsat_tile)

        # Assumes that file_list is 1D, it writes each path to a new line in the first 'column' of a .csv
        csv_output = tile_status_dir + '\\h99_for_processing\\' + str(lsat_tile) + '_h99_landsat_tile_list.csv'

        # Creates a csv list of the Landsat fractional cover image paths if the minimum fc_count threshold was met.
        with open(csv_output, "w") as output:
            writer = csv.writer(output, lineterminator='\n')
            for file in list_landsat_tile_path:
                writer.writerow([file])
    else:
        list_insufficient.append(lsat_tile)
        print('There are insufficient seasonal fractional cover h99 tiles for processing: ', str(lsat_tile))
        # sys.exit()

    # assumes that file_list is a flat list, it adds a
    csv_output2 = tile_status_dir + '\\h99_tile_status_lists\\' + 'Complete_list_of_h99_tiles_ready_for_zonal_stats.csv'
    # Creates a csv list of all the Landsat tile names that contain 1ha sites that have met the minimum
    # fc_count threshold.
    with open(csv_output2, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_sufficient:
            writer.writerow([file])

    csv_output3 = tile_status_dir + '\\h99_tile_status_lists\\' + 'Complete_list_of_h99_tiles_not_processed.csv'
    # Creates a csv list of all the Landsat tile names that contain 1ha sites that have NOT met the minimum
    # fc_count threshold.
    with open(csv_output3, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_insufficient:
            writer.writerow([file])

    return list_sufficient, list_landsat_tile_path


def main_routine(export_dir_path, geo_df2, image_count, lsat_dir, path, row, zone, extension):
    # define the tile_status_dir path
    tile_status_dir = (export_dir_path + '\\h99_tile_status')
    # print("tile_status_dir:", tile_status_dir)

    print("init landsat list")

    # Call the append_geo_df_fn function to concatenate previously separated projected 1ha sites to a single
    # geo-dataframe and re-project to geographic GDA94.
    geo_df = append_geo_df_fn(geo_df2, zone, export_dir_path)
    # print('geo_df: ', geo_df)
    # Call the unique_values_fn function to create a list of unique Landsat tiles that overlay with a 1ha site
    # - name restructured from geo-dataframe.
    # list_tile_unique = unique_values_fn(geo_df)
    # print("list_tile_unique: ", list_tile_unique)

    lsat_tile = str(path) + "_" + str(row)
    # call the create_csv_list_of_paths_fn function to determine which Landsat Tiles have a sufficient amount of
    # images to process.
    list_sufficient, list_landsat_tile_path = create_csv_list_of_paths_fn(lsat_tile, lsat_dir, extension,
                                                                          image_count, tile_status_dir, path, row, zone)

    # print(list_sufficient, geo_df)

    geo_df['uid'] = geo_df.index + 1
    print(geo_df.uid)

    return list_sufficient, geo_df, list_landsat_tile_path


if __name__ == "__main__":
    main_routine()
