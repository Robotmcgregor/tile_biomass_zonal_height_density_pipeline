#!/usr/bin/env python

# import modules
from __future__ import print_function, division

import fiona
import rasterio
import pandas as pd
from rasterstats import zonal_stats
import os
import shutil
import glob
import numpy as np
import geopandas as gpd
import warnings

warnings.filterwarnings("ignore")

'''
step1_6_dp0_landsat_list.py
================

Description: This script calculates the zonal statistics of multi, multi band images, from polygon shapefiles.
Author: Grant Staben
email: grant.staben@nt.gov.au
Date: zzzz
Version: 1.0


Modified: Rob McGregor
email: Robert.Mcgregor@nt.gov.au
Date: 27/10/2020
Version: 2.0

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

========================================================================================================================
'''

def apply_zonal_stats_fn(image_s, no_data, band, shape, uid):
    """ Collect the zonal statistical information fom a raster file contained within a polygon extend outputting a
    list of results (final_results).

        @param image_s: string object containing an individual path for each rainfall image as it loops through the
        cleaned imagery_list_image_results.
        @param no_data: integer object containing the raster no data value.
        @param band: string object containing the current band number being processed.
        @param shape: open odk shapefile containing the 1ha site polygons.
        @param uid: unique identifier number.
        @return final_results: list object containing all of the zonal stats, image and shapefile polygon/site
        information. """

    # create empty lists to append values
    zone_stats = []
    list_site = []
    list_uid = []
    list_prop = []
    list_prop_code = []
    list_site_date = []
    list_image_name = []
    image_date = []
    list_band = []

    with rasterio.open(image_s, nodata=no_data) as srci:
        affine = srci.transform
        array = srci.read(band)

        with fiona.open(shape) as src:
            # using 'all_touched=True' will increase the number of pixels used to produce the stats 'False'
            # reduces the number define the zonal stats being calculated
            zs = zonal_stats(src, array, affine=affine, nodata=no_data,
                             stats=['count', 'min', 'max', 'mean', 'median', 'std', 'percentile_25', 'percentile_50',
                                    'percentile_75', 'percentile_95', 'percentile_99', 'range'], all_touched=False)

            print("ccw: ", zs)
            # extract image name and append to list
            img_name = str(srci)[-54:-11]
            list_image_name.append(img_name)
            # extract image date and append to list
            img_date = str(srci)[-38:-30]
            image_date.append(img_date)

            for zone in zs:
                bands = 'b' + str(band)
                list_band.append(bands)
                # extract 'values' as a tuple from a dictionary
                keys, values = zip(*zone.items())
                # convert tuple to a list and append to zone_stats
                result = list(values)
                zone_stats.append(result)

            for i in src:
                # extract shapefile records
                table_attributes = i['properties']

                uid_ = table_attributes[uid]
                details = [uid_]
                list_uid.append(details)

                site = table_attributes['site_name']
                site_ = [site]
                list_site.append(site_)

        # join the elements in each of the lists row by row
        final_results = [list_uid + list_site + zone_stats for
                         list_uid, list_site, zone_stats in
                         zip(list_uid, list_site, zone_stats)]

        # close the vector and raster file 
        src.close()
        srci.close()

    return final_results, str(site_[0])


def time_stamp_fn(output_zonal_stats):
    """Insert a timestamp into feature position 4, convert timestamp into year, month and day strings and append to
    dataframe.

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats
    @return output_zonal_stats: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated features.
    """

    print(output_zonal_stats.date[:6])
    print(output_zonal_stats.date[6:])
    # Convert the date to a time stamp
    time_stamp_st = pd.to_datetime(output_zonal_stats.date[:6], format='%Y%m')
    time_stamp_end = pd.to_datetime(output_zonal_stats.date[6:], format='%Y%m')
    output_zonal_stats.insert(4, 'date_st', time_stamp_st)
    output_zonal_stats.insert(5, 'date_end', time_stamp_end)
    output_zonal_stats['year'] = output_zonal_stats['date'].map(lambda x: str(x)[:4])
    output_zonal_stats['month'] = output_zonal_stats['date'].map(lambda x: str(x)[4:6])
    output_zonal_stats['day'] = output_zonal_stats['date'].map(lambda x: str(x)[6:])

    return output_zonal_stats


def landsat_correction_fn(output_zonal_stats, num_bands, var_):
    """ Replace specific 0 values with Null values and correct b1, b2 and b3 calculations
    (refer to Fractional Cover metadata)

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats.
    @return: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated values.
    """
    print("var_: ", var_)
    for n in num_bands:
        i = str(n)
        output_zonal_stats['b{0}_{1}_min'.format(str(i), var_)] = \
            output_zonal_stats['b{0}_{1}_min'.format(str(i), var_)].replace(0, np.nan)

        # output_zonal_stats['b{0}_{1}_min'.format(i, var_)] = output_zonal_stats['b{0}_{1}_min'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_max'.format(i, var_)] = output_zonal_stats['b{0}_{1}_max'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_mean'.format(i, var_)] = output_zonal_stats['b{0}_{1}_mean'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_med'.format(i, var_)] = output_zonal_stats['b{0}_{1}_med'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_p25'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p25'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_p50'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p50'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_p75'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p75'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_p95'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p95'.format(i, var_)] - 100
        # output_zonal_stats['b{0}_{1}_p99'.format(i, var_)] = output_zonal_stats['b{0}_{1}_p99'.format(i, var_)] - 100
        ## output_zonal_stats['b{0}_{1}_range'.format(i, var_)] = output_zonal_stats['b{0}_{1}_range'.format(i, var_)] - 100
        #

    return output_zonal_stats

def time_stamp_fn(output_zonal_stats):
    """Insert a timestamp into feature position 4, convert timestamp into year, month and day strings and append to
    dataframe.

    @param output_zonal_stats: dataframe object containing the Landsat tile Fractional Cover zonal stats
    @return output_zonal_stats: processed dataframe object containing the Landsat tile Fractional Cover zonal stats and
    updated features.
    """

    s_year_ = []
    s_month_ = []
    s_day_ = []
    s_date_ = []
    e_year_ = []
    e_month_ = []
    e_day_ = []
    e_date_ = []

    import calendar
    print("init time stamp")
    # Convert the date to a time stamp
    for n in output_zonal_stats.date:
        i = str(n)
        #print(i)
        s_year = i[:4]
        s_month = i[4:6]
        s_day = "01"
        s_date = str(s_year) + str(s_month) + str(s_day)

        s_year_.append(s_year)
        s_month_.append(s_month)
        s_day_.append(s_day)
        s_date_.append(s_date)

        e_year = i[6:10]
        e_month = i[10:12]
        m, d = calendar.monthrange(int(e_year), int(e_month))
        e_day = str(d)
        if len(e_day) < 1:
            d_ = "0" + str(d)
        else:
            d_ = str(d)

        e_date = str(e_year) + str(e_month) + str(d_)

        e_year_.append(e_year)
        e_month_.append(e_month)
        e_day_.append(e_day)
        e_date_.append(e_date)

    output_zonal_stats.insert(4, 'e_date', e_date_)
    output_zonal_stats.insert(4, 'e_year', e_year_)
    output_zonal_stats.insert(4, 'e_month', e_month_)
    output_zonal_stats.insert(4, 'e_day', e_day_)

    output_zonal_stats.insert(4, 's_date', s_date_)
    output_zonal_stats.insert(4, 's_year', s_year_)
    output_zonal_stats.insert(4, 's_month', s_month_)
    output_zonal_stats.insert(4, 's_day', s_day_)

    pd.to_datetime(output_zonal_stats.s_date, format='%Y%m%d')

    pd.to_datetime(output_zonal_stats.e_date, format='%Y%m%d')

    return output_zonal_stats



def main_routine(temp_dir_path, zonal_stats_ready_dir, no_data, tile, zonal_stats_output, shape, var_):

    """Restructure ODK 1ha geo-DataFrame to calculate the zonal statistics for each 1ha site per Landsat Fractional
    Cover image, per band (b1, b2 and b3). Concatenate and clean final output DataFrame and export to the Export
    directory/zonal stats."""

    print('step1_6_ccw_zonal_stats.py INITIATED.')

    print("tile: ", tile)
    _, f = os.path.split(tile)
    print("f: ", f)

    tile_begin = f[:3]
    print("tile_begin: ", tile_begin)
    tile_end = f[4:7]
    print("tile end: ", tile_end)
    complete_tile = tile_begin + tile_end
    print('='*50)
    print('Working on tile: ', complete_tile)
    print('=' * 50)
    print('......')

    # shapefile = os.path.join(zonal_stats_ready_dir, "{0}_by_tile.shp".format(complete_tile))
    # df = gpd.read_file("Z:\\Scratch\\Rob\\test2.shp")

    # shape = shapefile
    #shape = geo_df3
    # nodata = int(0)
    uid = 'uid'
    im_list = tile

    # create temporary folders
    ccw_temp_dir_bands = os.path.join(temp_dir_path, 'ccw_temp_individual_bands')
    os.makedirs(ccw_temp_dir_bands)

    num_bands = [1]#, 2, 3]
    for i in num_bands:

        band_dir = os.path.join(ccw_temp_dir_bands, 'band{0}'.format(str(i)))
        os.makedirs(band_dir)

    for band in num_bands:
        # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
        with open(im_list, 'r') as imagery_list:

            # Extract each image path from the image list
            for image in imagery_list:

                # cleans the file pathway (Windows)
                image_s = image.rstrip()
                path_, im_name = os.path.split(image_s) #[
                #            -43:]  # May need to change these values depending on whether there is a 2 or 3 in the
                # # name.

                #print("im_name_s: ", im_name_s)
                #im_name = im_name_s + 'g'
                # print('Image name: ', im_name)

                image_name_split = im_name.split("_")
                im_date = image_name_split[-2][1:]
                #print("im_date: ", im_date)
                # im_date = image_s[
                #           -27:-19]  # May need to change these values depending on whether there is a 2 or 3 in the
                # # name.
                #print("im_date: ", im_date)
                # loops through each image
                with rasterio.open(image_s, nodata=no_data) as srci:
                    image_results = 'image_' + im_name[:-4] + '.csv'

                    # runs the zonal stats function and outputs a csv in a band specific folder
                    final_results, site_name = apply_zonal_stats_fn(image_s, no_data, band, shape, uid)

                    # header = [str(band) + '_number', str(band) + '_site', str(band) + '_min', str(band) + '_max',
                    #           str(band) + '_mean', str(band) + '_count', str(band) + '_std', str(band) + '_median']
                    #

                    header = ["b" + str(band) + '_uid', "b" + str(band) + '_site', "b" + str(band) + '_min',
                              "b" + str(band) + '_max',  "b" + str(band) + '_mean',  "b" + str(band) + '_count',
                              "b" + str(band) + '_std', "b" + str(band) + '_median', "b" + str(band) + '_range',
                              "b" + str(band) + '_p25', "b" + str(band) + '_p50', "b" + str(band) + '_p75',
                              "b" + str(band) + '_p95', "b" + str(band) + '_p99']

                    if band == 1:

                        df1 = pd.DataFrame.from_records(final_results)
                        print(df1)
                        df = pd.DataFrame.from_records(final_results, columns=header)
                        df['band'] = band
                        df['image'] = im_name
                        df['date'] = im_date
                        print(df)
                        df.to_csv(ccw_temp_dir_bands + '//band1//' + image_results, index=False)
                    # elif band == 2:
                    #     df = pd.DataFrame.from_records(final_results, columns=header)
                    #     df['band'] = band
                    #     df['image'] = im_name
                    #     df['date'] = im_date
                    #     df.to_csv(ccw_temp_dir_bands + '//band2//' + image_results, index=False)
                    # elif band == 3:
                    #     df = pd.DataFrame.from_records(final_results, columns=header)
                    #     df['band'] = band
                    #     df['image'] = im_name
                    #     df['date'] = im_date
                    #     df.to_csv(ccw_temp_dir_bands + '//band3//' + image_results, index=False)
                    else:
                        print('There is an error.')

    # -------------------------------------------------- Concatenate csv -----------------------------------------------

    # for loops through the band folders and concatenates zonal stat outputs into a complete band specific csv
    for x in num_bands:
        location_output = ccw_temp_dir_bands + '//band' + str(x)
        band_files = glob.glob(os.path.join(location_output,
                                            '*.csv'))

        # advisable to use os.path.join as this makes concatenation OS independent
        df_from_each_band_file = (pd.read_csv(f) for f in band_files)
        concat_band_df = pd.concat(df_from_each_band_file, ignore_index=False, axis=0, sort=False)
        # export the band specific results to a csv file (i.e. three outputs)
        concat_band_df.to_csv(ccw_temp_dir_bands + '//' + 'Band' + str(x) + '_test.csv', index=False)

    # ----------------------------------------- Concatenate three bands together ---------------------------------------

    # Concatenate Three bands
    header_all = ['uid', 'site', 'b1_ccw_min', 'b1_ccw_max', 'b1_ccw_mean', 'b1_ccw_count',
                  'b1_ccw_std', 'b1_ccw_med', 'b1_ccw_range', 'b1_ccw_p25', 'b1_ccw_p50', 'b1_ccw_p75',
                              'b1_ccw_p95', 'b1_ccw_p99',  'band', 'image', 'date']
    # print("ccw_temp_dir_bands: ", ccw_temp_dir_bands)

    all_files = glob.glob(os.path.join(ccw_temp_dir_bands,
                                       '*.csv'))
    # advisable to use os.path.join as this makes concatenation OS independent
    df_from_each_file = (pd.read_csv(f) for f in all_files)
    output_zonal_stats = pd.concat(df_from_each_file, ignore_index=False, axis=1, sort=False)

    print(output_zonal_stats)
    for i in output_zonal_stats.columns:
        print(i)
    output_zonal_stats.columns = header_all

    # -------------------------------------------------- Clean dataframe -----------------------------------------------

    # Convert the date to a time stamp
    output_zonal_stats = time_stamp_fn(output_zonal_stats)

    # remove 100 from zone_stats
    output_zonal_stats = landsat_correction_fn(output_zonal_stats, num_bands, var_)

    # reshape the final dataframe
    output_zonal_stats = output_zonal_stats[
        ['uid', 'site', 'image', 's_day', 's_month', 's_year', 's_date', 'e_day', 'e_month', 'e_year', 'e_date', 'b1_ccw_min',
         'b1_ccw_max', 'b1_ccw_mean', 'b1_ccw_count', 'b1_ccw_std', 'b1_ccw_med', 'b1_ccw_p25', 'b1_ccw_p50', 'b1_ccw_p75',
                                  'b1_ccw_p95', 'b1_ccw_p99', 'b1_ccw_range']]

    site_list = output_zonal_stats.site.unique().tolist()
    print("length of site list: ", len(site_list))
    if len(site_list) >= 1:
        for i in site_list:
            out_df = output_zonal_stats[output_zonal_stats['site'] == i]

            out_path = os.path.join(zonal_stats_output, "{0}_{1}_ccw_zonal_stats.csv".format(str(i), complete_tile))
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(zonal_stats_output, "{0}_{1}_ccw_zonal_stats.csv".format(str(site_list[0]), complete_tile))
        # export the pandas df to a csv file
        output_zonal_stats.to_csv(out_path, index=False)


    # ----------------------------------------------- Delete temporary files -------------------------------------------
    # remove the temp dir and single band csv files
    shutil.rmtree(ccw_temp_dir_bands)

    print('=' * 50)

    return output_zonal_stats, complete_tile, tile, ccw_temp_dir_bands


if __name__ == '__main__':
    main_routine()
