#!/usr/bin/env python


"""
Landsat Tile Grid Identify.py
=============================================

Description: This script preforms a spatial overlay using Geopandas to identify which sites spatially overlay or touch
which Landsat tile.

Step 1: Divides the Landsat tile grid into two based on its WGS zonal position.

Step 2: Applies a negative 4000m buffer to each Landsat tile to reduce noise, and concatenates them into two shapefiles
(projectedDF2 and projectedDF3)

Step 3: Using GeoPandas, preforms a spatial overlay (identify) is preformed between the Landsat Tile Grid and the
completed ODK sites shapefile to determine which Landsat tile should be used to derive zonal statistics from, on a site
by site basis. Following this, the identity shapefiles are concatenated and two outputs are created
(comp_geo_df_52 and comp_geo_df_53).


Author: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 1.0

###############################################################################################

MIT License

Copyright (c) 2020 Rob McGregor

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

########################################################################################################################

========================================================================================================
"""

# Import modules
from __future__ import print_function, division
import os
import geopandas as gpd
import pandas as pd
import glob
import warnings
import sys

warnings.filterwarnings("ignore")


def project_tile_grid_fn(tile_grid, prime_temp_grid_dir):
    """ Subset and re-project the Landsat tile grid into WGS zone 52 and 53.

    @param tile_grid: geo-dataframe containing the Landsat tile locations and names.
    @param prime_temp_grid_dir: string object containing the path to the temporary directory.
    @return tile_grid_wgs52: geo-dataframe object filtered based on utm zone from the Landsat tile grid.
    @return tile_grid_wgs53: geo-dataframe object filtered based on utm zone from the Landsat tile grid.
    """
    proj_tile_grid_sep_dir = prime_temp_grid_dir + '\\separation'

    # read in Landsat tile grid vector dataset
    tile_grid = gpd.read_file(tile_grid)
    # subset dataset into WGSz52 and WGSz53
    # tile_grid_54_selection = tile_grid.loc[tile_grid['WRSPR'] == 100_074]
    # tile_grid_53_selection = tile_grid.loc[(tile_grid['WRSPR'] <= 104_073) & ((tile_grid['WRSPR'] != 103_078) |(tile_grid['WRSPR'] != 100_074))]
    # tile_grid_52_selection = tile_grid.loc[(tile_grid['WRSPR'] >= 104_072) | (tile_grid['WRSPR'] == 103_078)]

    tile_grid_54_selection = tile_grid.loc[(tile_grid['WRSPR'] <= 100_074) & (tile_grid["WRSPR"]>=100_072)]
    tile_grid_53_selection = tile_grid.loc[(tile_grid['WRSPR'] <= 104_073) & ((tile_grid['WRSPR'] != 103_078) |(tile_grid['WRSPR'] != 100_074) |(tile_grid['WRSPR'] != 100_073) |(tile_grid['WRSPR'] != 100_072))]
    tile_grid_52_selection = tile_grid.loc[(tile_grid['WRSPR'] >= 104_072) | (tile_grid['WRSPR'] == 103_078)]

    # project subsets into crs
    tile_grid_wgs52 = tile_grid_52_selection.to_crs(epsg=32752)
    tile_grid_wgs53 = tile_grid_53_selection.to_crs(epsg=32753)
    tile_grid_wgs54 = tile_grid_54_selection.to_crs(epsg=32754)

    # export shapefiles  
    tile_grid_wgs52.to_file(driver='ESRI Shapefile', filename=proj_tile_grid_sep_dir + '\\tile_grid_wgs52.shp')
    tile_grid_wgs53.to_file(driver='ESRI Shapefile', filename=proj_tile_grid_sep_dir + '\\tileGridWgs53.shp')
    tile_grid_wgs54.to_file(driver='ESRI Shapefile', filename=proj_tile_grid_sep_dir + '\\tileGridWgs54.shp')

    return tile_grid_wgs52, tile_grid_wgs53, tile_grid_wgs54


def negative_buffer_fn(projected_df, prime_temp_grid_dir, crs_name):
    """ Separate each Landsat tile, apply a negative buffer (4000m) and export the shapefiles.

    @param projected_df: geo-dataframe containing the filtered version of the Landsat tile grid.
    @param prime_temp_grid_dir: string object containing the path to the temporary directory.
    @param crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    @return tile_grid_temp_dir: string object containing the path to sub-directory tile_grid/crs_name.
    @return crs_name: string object containing the standardised crs information to be used as part of the file name.
    """
    tile_grid_temp_dir = prime_temp_grid_dir + '\\tile_grid\\' + crs_name
    os.makedirs(tile_grid_temp_dir)

    # Loop through the unique values within the projected_df feature: WRSPR
    for landsatTile in projected_df.WRSPR.unique():
        # subset the projected_df geoDataFrame based on the unique Landsat tile variable.
        projected_df2 = projected_df.loc[projected_df.WRSPR == landsatTile]

        # apply a negative 4000m buffer from each Landsat tile to mask the tile images at a later point.

        #todo buffer should be -4000 - buff01 is missing so changed to -3000 for tile 106069
        projected_df3 = projected_df2.buffer(-3000)

        # export shapefile.
        projected_df3.to_file(driver='ESRI Shapefile',
                              filename=tile_grid_temp_dir + '\\' + str(landsatTile) + '_NegBuffer_' + crs_name + '.shp')

    return tile_grid_temp_dir, crs_name


def concatenate_df_fn(prime_temp_grid_dir, tile_grid_temp_dir, crs_name):
    """ Create one geoDataFrame (comp_tile_geo_df) containing all negatively buffered Landsat tiles identified as
    overlaying an odk 1ha site within their respective WGS84 zones.

    @param prime_temp_grid_dir: string object containing the path to the temporary directory.
    @param tile_grid_temp_dir: string object containing the path to sub-directory tile_grid/crs_name.
    @param crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    @return comp_tile_geo_df: geo-dataframe produced from all located shapefiles.
    @return concat_tile_grid_temp_dir: string object containing the path to the export temporary sub-directory
    @return crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    """
    # Create two folders for outputs
    concat_tile_grid_temp_dir = prime_temp_grid_dir + '\\concat_tile_grid\\' + crs_name
    os.makedirs(concat_tile_grid_temp_dir)

    # create an empty list     
    list_df = []

    for landsat_tile_shp in glob.glob(tile_grid_temp_dir + '\\*.shp'):
        geo_df = gpd.read_file(landsat_tile_shp)
        list_df.append(geo_df)

    if len(list_df) >= 2:

        comp_tile_geo_df = gpd.GeoDataFrame(pd.concat(list_df, ignore_index=True), crs=list_df[0].crs)
        comp_tile_geo_df.to_file(concat_tile_grid_temp_dir + '\\comp_geo_df_buffer_' + crs_name + '.shp')

    elif len(list_df) == 1:

        comp_tile_geo_df = gpd.read_file(landsat_tile_shp, ignore_index=True)
        comp_tile_geo_df.to_file(concat_tile_grid_temp_dir + '\\comp_geo_df_buffer_' + crs_name + '.shp')
    else:
        print('There are no files: concatenate_df_fn')
        sys.exit(1)
        comp_tile_geo_df = None

    return comp_tile_geo_df, concat_tile_grid_temp_dir, crs_name


def identity_df_fn(tile_grid_temp_dir, prime_temp_grid_dir, odk_geo_1ha_df, crs_name):
    """ Identify which site spatially overlays which Landsat tile and output a shapefile with the tile name in the
    filename.

    @param tile_grid_temp_dir: string object containing the path to sub-directory tile_grid/crs_name.
    @param prime_temp_grid_dir: string object containing the path to the temporary directory.
    @param odk_geo_1ha_df:
    @param crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    @return identify_tile_grid_temp_dir: string object containing the path to sub-directory identify_tile_grid\crs_name.
    """
    identify_tile_grid_temp_dir = prime_temp_grid_dir + '\\identify_tile_grid\\' + crs_name
    os.makedirs(identify_tile_grid_temp_dir)

    # create an empty list
    list_df = []

    for file in glob.glob(tile_grid_temp_dir + '\\*.shp'):
        # Extract Landsat tile name
        tile = str(file[-29:-23])
        # code_str = str(code)
        clean_tile = tile.replace('\\', '0')
        # Append the open geo_df to a list.
        geo_df = gpd.read_file(file)
        list_df.append(geo_df)
        intersect_df = gpd.overlay(geo_df, odk_geo_1ha_df, how='identity')
        intersect_df.to_file(identify_tile_grid_temp_dir + '\\' + clean_tile + '_identity_' + crs_name + '.shp')

    return identify_tile_grid_temp_dir


def concatenate_tile_df_fn(zonal_stats_ready_dir, identify_tile_grid_temp_dir, prime_temp_grid_dir, crs_name):
    """ Create a geoDataFrame for all of the shapefiles created in the identity_df_fn function through concatenation.

    @param zonal_stats_ready_dir: string object containing the path to a temporary sub-directory
    prime_temp_grid_dir\zonal_stats_ready\crs_name.
    @param identify_tile_grid_temp_dir: string object containing the path to sub-directory identify_tile_grid\crs_name.
    @param prime_temp_grid_dir: string object containing the path to the temporary directory.
    @param crs_name: string object containing the standardised crs information to be used as part of the file/sub-dir.
    @return comp_geo_df: geo-dataframe
    """

    concat_identity_tile_grid_dir = prime_temp_grid_dir + '\\concat_identity_tile_grid\\' + crs_name
    os.makedirs(concat_identity_tile_grid_dir)

    list_identify_zone = []

    for file in glob.glob(identify_tile_grid_temp_dir + "\\*" + crs_name + ".shp"):
        # append the open geo_df to a list.
        print("file: ", file)
        geo_df = gpd.read_file(file)
        clean_tile = file[-28:-22]
        geo_df['TILE'] = clean_tile
        list_identify_zone.append(geo_df)

    if len(list_identify_zone) >= 1:
        geo_df1 = gpd.GeoDataFrame(pd.concat(list_identify_zone, ignore_index=True), crs=list_identify_zone[0].crs)
        comp_geo_df = geo_df1.dropna(axis=0, subset=['FID_2'])
        comp_geo_df.rename(columns={"TILE": "tile"}, errors="raise", inplace=True)
        print('comp_geo_df: ', comp_geo_df)
        #comp_geo_df.to_file(r"Z:\Scratch\Rob\tern\tree_biomass_field_data\test.shp", driver="ESRI Shapefile")

        for i in comp_geo_df.tile.unique():
            print("i: ", i)
            site_tile_df = comp_geo_df.loc[comp_geo_df.tile == i]
            site_tile_df2 = site_tile_df[['site_name', 'tile', 'geometry']]
            site_tile_df2.reset_index(drop=True, inplace=True)
            site_tile_df2['uid'] = site_tile_df2.index + 1
            #export_file = os.path.join(zonal_stats_ready_dir,  str(i) + '_by_tile.shp')
            site_tile_df2.to_file(zonal_stats_ready_dir + '\\' + str(i) + '_by_tile.shp')
    else:
        print("-------------------------ERROR--------------------------------")
        sys.exit(1)
        comp_geo_df = None

    return comp_geo_df


def main_routine(tile_grid, geo_df2, data, zone, export_dir_path, prime_temp_grid_dir):
    #tile_grid, geo_df52, geo_df53, prime_temp_grid_dir):

    print(zone)


    # define the zonal_stats_ready_dir path
    zonal_stats_ready_dir = prime_temp_grid_dir + '\\zonal_stats_ready'

    # call the project_tile_grid_fn function.
    tile_grid_wgs52, tile_grid_wgs53, tile_grid_wgs54 = project_tile_grid_fn(tile_grid, prime_temp_grid_dir)


    if zone == "2":
    # ------------------------------------------ tile_grid_wgs52 -------------------------------------------------------

        # set the projected_df to tile_grid_wgs52
        projected_df = tile_grid_wgs52
        # set the crs_name variable to 'WGS84z52'
        crs_name = 'WGS84z52'
        # call the negative_buffer_fn function.
        tile_grid_temp_dir, crs_name = negative_buffer_fn(projected_df, prime_temp_grid_dir, crs_name)
        # set the odk_geo1ha_df variable to geo_df52
        odk_geo1ha_df = geo_df2

        print("odk_geo1ha_df: ", odk_geo1ha_df)
        # call the concatenate_df_fn function.
        comp_tile_geo_df, concat_tile_grid_temp_dir, crs_name = concatenate_df_fn(prime_temp_grid_dir, tile_grid_temp_dir,
                                                                                  crs_name)
        # call the identity_df_fn function.
        identify_tile_grid_temp_dir = identity_df_fn(tile_grid_temp_dir, prime_temp_grid_dir, odk_geo1ha_df, crs_name)
        # call the concatenate_tile_df_fn function.
        comp_geo_df = concatenate_tile_df_fn(zonal_stats_ready_dir, identify_tile_grid_temp_dir, prime_temp_grid_dir,
                                               crs_name)

    elif zone == "3":

        # -------------------------------------------- tile_grid_wgs53 --------------------------------------------------

        projected_df = tile_grid_wgs53
        crs_name = 'WGS84z53'
        # call the negative_buffer_fn function.
        tile_grid_temp_dir, crs_name = negative_buffer_fn(projected_df, prime_temp_grid_dir, crs_name)
        # set the odk_geo1ha_df variable to geo_df53
        odk_geo1ha_df = geo_df2
        # call the concatenate_df_fn function.
        comp_tile_geo_df, concat_tile_grid_temp_dir, crs_name = concatenate_df_fn(prime_temp_grid_dir, tile_grid_temp_dir,
                                                                                  crs_name)
        # call the identifyDF function.
        identify_tile_grid_temp_dir = identity_df_fn(tile_grid_temp_dir, prime_temp_grid_dir, odk_geo1ha_df, crs_name)
        # call the concatenate_tile_df_fn function.
        comp_geo_df = concatenate_tile_df_fn(zonal_stats_ready_dir, identify_tile_grid_temp_dir, prime_temp_grid_dir,
                                               crs_name)

    elif zone == "4":

        # -------------------------------------------- tile_grid_wgs54 --------------------------------------------------

        projected_df = tile_grid_wgs54
        crs_name = 'WGS84z54'
        # call the negative_buffer_fn function.
        tile_grid_temp_dir, crs_name = negative_buffer_fn(projected_df, prime_temp_grid_dir, crs_name)
        # set the odk_geo1ha_df variable to geo_df53
        odk_geo1ha_df = geo_df2
        # call the concatenate_df_fn function.
        comp_tile_geo_df, concat_tile_grid_temp_dir, crs_name = concatenate_df_fn(prime_temp_grid_dir, tile_grid_temp_dir,
                                                                                  crs_name)
        # call the identifyDF function.
        identify_tile_grid_temp_dir = identity_df_fn(tile_grid_temp_dir, prime_temp_grid_dir, odk_geo1ha_df, crs_name)
        # call the concatenate_tile_df_fn function.
        comp_geo_df = concatenate_tile_df_fn(zonal_stats_ready_dir, identify_tile_grid_temp_dir, prime_temp_grid_dir,
                                               crs_name)


    return comp_geo_df, zonal_stats_ready_dir


if __name__ == "__main__":
    main_routine()
