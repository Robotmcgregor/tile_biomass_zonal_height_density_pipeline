#!/usr/bin/env python

"""
step1_5_dil_landsat_list.py
================
Description: This script searches through each Landsat tile directory that was identified as overlaying with an odk 1hs
site and determines if there are sufficient images for zonal stats processing (greater than fc_count).
If an identified tile contains sufficient  images, each image path will be input into a csv (1 path per line) and the
csv will be saved in the for processing sub-directory. If there are insufficient images then the tile name will be saved
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
import warnings

warnings.filterwarnings("ignore")


def append_geo_df_fn(comp_geo_df_52, comp_geo_df_53, export_dir_path):
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
    geo_df_gda941 = comp_geo_df_52['crs'] = 'WGSz52'
    geo_df_gda942 = comp_geo_df_53['crs'] = 'WGSz53'

    # Project both geoDataFrames to geographic GDA94.
    geo_df_gda941 = comp_geo_df_52.to_crs(epsg=4283)
    geo_df_gda942 = comp_geo_df_53.to_crs(epsg=4283)

    # Append/concatenate both geoDataFrames into one.
    geo_df = geo_df_gda941.append(geo_df_gda942)

    # Export geoDataFrame to the export directory (command argument).
    geo_df.to_file(driver='ESRI Shapefile', filename=export_dir_path + '\\' + 'landsat_tile_site_identity_gda94.shp')

    return geo_df


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


def list_file_directory_fn(landsat_tile_dir, image_search_criteria1, image_search_criteria2):
    """ Create an empty list to store the Landsat image file path for images that meet the search criteria
    (image_search_criteria1 and image_search_criteria2).
    @param landsat_tile_dir:
    @param image_search_criteria1: string object containing the end part of the required file name (--search_criteria1)
    @param image_search_criteria2: string object containing the end part of the required file name (--search_criteria2)
    @return list_landsat_tile_path: list object containing the path to all matching either search criteria.
    """
    # Create an empty list to store file paths.
    list_landsat_tile_path = []
    #print('image_search_criteria1: ', image_search_criteria1)
    #print('image_search_criteria2: ', image_search_criteria2)
    # Navigate and loop through the folders within the Landsat Tile Directory stored in the 'landsat_tile_dir'
    # object variable.
    for root, dirs, files in os.walk(landsat_tile_dir):
        for file in files:
            #print('file: ', file)
            # Search for files ending with the string value stored in the object variable: imageSearchCriteria.
            if file.endswith(image_search_criteria1) or file.endswith(image_search_criteria2):
                # Concatenate the root and file names to create a file path.
                image_path = (os.path.join(root, file))
                #print('image_path: ', image_path)
                # Append the image_path variable to the empty list 'list_landsat_tile_path'.
                list_landsat_tile_path.append(image_path)
                #print("list_landsat: ", list_landsat_tile_path)
    return list_landsat_tile_path


def create_csv_list_of_paths_fn(list_tile_unique, landsat_dir, image_search_criteria1, image_search_criteria2, fc_count,
                                tile_status_dir):
    """ Determine which Landsat Tiles have a sufficient amount of images to process.

    @param list_tile_unique: list object containing the path to all landsat images matching either search criteria.
    @param landsat_dir: string object to the Landsat tile sub-directory of interest.
    @param image_search_criteria1: string object containing the end part of the required file name (--search_criteria1)
    @param image_search_criteria2: string object containing the end part of the required file name (--search_criteria2)
    @param fc_count: integer object containing the command argument --image_count
    @param tile_status_dir: string object to the sub-directory export_dir\tile_status
    @return list_sufficient: list object containing the the path to all Landsat images of interest providing that the
    number was greater than the fc_count value.
    """
    # Crete two empty list to contain Landsat tile names which meet and do not meet the minimum number of images set
    # by the 'fc_count' variable (command argument: fc_count).

    list_insufficient = []
    list_sufficient = []

    for landsat_tile in list_tile_unique:
        # Loop through the unique Landsat Tile list ' listTile Unique'.
        landsat_tile_dir = landsat_dir + '\\' + landsat_tile
        print('=' * 50)
        print('Confirm that there are sufficient fractional cover tiles for processing')
        print('landsat_tile_dir: ', landsat_tile_dir)
        # Run the list_file_directory_fn function.
        list_landsat_tile_path = list_file_directory_fn(landsat_tile_dir, image_search_criteria1,
                                                        image_search_criteria2)

        # Calculate the number of image pathways stored in the list_landsat_tile_path variable.
        fc_length = (len(list_landsat_tile_path))
        print(' - Total fractional cover tiles located: ', fc_length)

        # Rule set to file Landsat tile names based on the amount of images comparatively to the minimum requirement
        # set by the fc_count variable.
        print(' - Minimum tiles (command argument): ', fc_count)
        print('=' * 50)
        if fc_length >= fc_count:
            # Append landsat_tile to list_sufficient if the number of images are equal to or more that the minimum
            # amount defined in the 'fc_count' variable.
            list_sufficient.append(landsat_tile)

            # Assumes that file_list is 1D, it writes each path to a new line in the first 'column' of a .csv
            csv_output = tile_status_dir + '\\fpc_for_processing\\' + str(landsat_tile) + '_fpc_landsat_tile_list.csv'

            # Creates a csv list of the Landsat fractional cover image paths if the minimum fc_count threshold was met.
            with open(csv_output, "w") as output:
                writer = csv.writer(output, lineterminator='\n')
                for file in list_landsat_tile_path:
                    writer.writerow([file])
        else:
            list_insufficient.append(landsat_tile)
            print('There are insufficient Landsat images for: ', str(landsat_tile))
            #sys.exit()

    # assumes that file_list is a flat list, it adds a
    csv_output2 = tile_status_dir + '\\fpc_tile_status_lists\\' + 'Complete_list_of_fpc_tiles_ready_for_zonal_stats.csv'
    # Creates a csv list of all of the Landsat tile names that contain 1ha sites that have met the minimum
    # fc_count threshold.
    with open(csv_output2, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_sufficient:
            writer.writerow([file])

    csv_output3 = tile_status_dir + '\\fpc_tile_status_lists\\' + 'Complete_list_of_fpc_tiles_not_processed.csv'
    # Creates a csv list of all of the Landsat tile names that contain 1ha sites that have NOT met the minimum
    # fc_count threshold.
    with open(csv_output3, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        for file in list_insufficient:
            writer.writerow([file])

    return list_sufficient


def main_routine(export_dir_path, comp_geo_df52, comp_geo_df53, fc_count, landsat_dir, image_search_criteria1,
                 image_search_criteria2):

    # define the tile_status_dir path
    tile_status_dir = (export_dir_path + '\\fpc_tile_status')
    print("tile_status_dir:", tile_status_dir)

    # Call the append_geo_df_fn function to concatenate previously separated projected 1ha sites to a single
    # geo-dataframe and re-project to geographic GDA94.
    geo_df = append_geo_df_fn(comp_geo_df52, comp_geo_df53, export_dir_path)

    # Call the unique_values_fn function to create a list of unique Landsat tiles that overlay with a 1ha site
    # - name restructured from geo-dataframe.
    list_tile_unique = unique_values_fn(geo_df)

    # call the create_csv_list_of_paths_fn function to determine which Landsat Tiles have a sufficient amount of
    # images to process.
    list_sufficient = create_csv_list_of_paths_fn(list_tile_unique, landsat_dir, image_search_criteria1,
                                                  image_search_criteria2,
                                                  fc_count, tile_status_dir)

    return list_sufficient, geo_df


if __name__ == "__main__":
    main_routine()
