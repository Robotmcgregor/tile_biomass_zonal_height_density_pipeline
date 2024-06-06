#!/usr/bin/env python

from __future__ import print_function, division
import fiona
import rasterio
import pandas as pd
from rasterstats import zonal_stats
import geopandas as gpd
import warnings
import os
from glob import glob
import numpy as np
import calendar
import shutil

warnings.filterwarnings("ignore")

'''
step1_10_seasonal_dka_zonal_stats.py
============================

Read in dka Landsat mosaics and extract zonal statistics for each 1ha plot.
Returns a csv file containing the statistics for each site for all files located within the specified directory.

Modified: Rob McGregor
email: robert.mcgregor@nt.gov.au
Date: 25/10/2022
version 1.0


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
'''


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

    # import calendar
    print("init time stamp")
    # Convert the date to a time stamp

    # output_zonal_stats.astype({'date': 'string'}).dtypes

    for n in output_zonal_stats.date:
        print("n: ", n)
        i = str(n)
        print("i: ", i)
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


def project_shapefile_gcs_wgs84_fn(albers, geo_df):
    """ Re-project a shapefile to 'GCSWGS84' to match the projection of the max_temp data.
    @param gcs_wgs84_dir: string object containing the path to the subdirectory located in the temporary_dir\gcs_wgs84
    @return:
    """

    # read in shp as a geoDataFrame.
    # df = gpd.read_file(zonal_stats_ready_dir + '\\' + complete_tile + '_by_tile.shp')

    # project to Australian Albers
    cgs_df = geo_df.to_crs(epsg=3577)

    # define crs file/path name variable.
    crs_name = 'albers'

    # Export re-projected shapefiles.
    projected_shape_path = albers + '\\' + 'geo_df_' + str(crs_name) + '.shp'

    # Export re-projected shapefiles.
    cgs_df.to_file(projected_shape_path)

    return cgs_df, projected_shape_path


def apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable, no_data, dis_temp_dir_bands):
    """
    Derive zonal stats for a list of Landsat imagery.

    @param image_s: string object containing the file path to the current max_temp tiff.
    @param projected_shape_path: string object containing the path to the current 1ha shapefile path.
    @param uid: ODK 1ha dataframe feature (unique numeric identifier)
    @return final_results: list object containing the specified zonal statistic values.
    """
    # create empty lists to write in  zonal stats results 

    print("+" * 50)
    zone_stats_list = []
    site_id_list = []
    image_name_list = []
    # print("Working on variable: ", variable)
    # print(qld_dict)
    # variable_values = qld_dict.get(variable)

    # print("variable_values: ", variable_values)
    no_data = no_data  # the no_data value for the silo max_temp raster imagery

    df_list = []
    # create empty lists to append values
    image_name = []
    image_date = []
    site_li = []
    zoneclass = []
    zoneresults = []
    total_uid_list = []
    laiskey_list = []
    total_laiskey_list = []
    property_list = []
    im_name_list = []
    im_date_list = []
    uid_list = []
    site_list = []

    with rasterio.open(image_s, nodata=no_data) as srci:
        # image_results = 'image_' + im_name + '.csv'

        affine = srci.transform
        array = srci.read(1)

        # array = array - 100

        # open the 'GCSWGS84' projected shapefile (1ha sites)
        with fiona.open(projected_shape_path) as src:

            cmap = {1: 'jan', 2: 'feb', 3: 'mar', 4: 'april', 5: 'may', 6: 'june',
                    7: 'july', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'}

            zs = zonal_stats(src, array, affine=affine, nodata=no_data,
                             stats=['count', 'min', 'max', 'mean', 'sum', 'std', 'median', 'majority', 'minority'],
                             categorical=True, category_map=cmap, all_touched=True)

            print(zs)

            path_, im_name = os.path.split(image_s)
            print("path_: ", path_)
            print("im_name: ", im_name)
            im_name_list.append(im_name)

            image_name_split = im_name.split("_")
            print(len(image_name_split[-2]))


            if str(image_name_split[-2]).startswith("m"):
                print("seasonal")
                im_date = str(image_name_split[-2][1:])
                im_date_st = str(im_date)

            elif len(image_name_split[-2])==4:
                print("annual date")
                im_date = str(image_name_split[-2])
                im_date_st = str(im_date)

            else:
                print("single date")
                im_date = str(image_name_split[-2])
                im_date_st = str(im_date)

            print("im_date: ", im_date)
            im_date_list.append(str(im_date))

            df = pd.DataFrame.from_records(zs)

            df.insert(0, 'dka_image', im_name)
            df.insert(0, 'date', str(im_date_st))
            print("-" * 50)
            print("df: ", df)
            print("df shape: ", df.shape)

            # extract out the site number for the polygon

            for i in src:
                table_attributes = i['properties']  # reads in the attribute table for each record

                ident = table_attributes[
                    uid]
                uid_list.append(
                    ident)  # reads in the id field from the attribute table and prints out the selected record

                site = table_attributes['site_name']
                site_list.append(site)

                # details = [ident, site, im_date]

                # site_id_list.append(details)
                # image_used = [file_name_final]
                image_name_list.append(im_name)

            df["uid"] = uid_list
            df["site"] = site_list
            band = 1
            df["band"] = 1

            header = ['uid', 'site', 'count', 'min', 'max', 'mean', 'sum', 'std', 'median', 'majority', 'minority',
                      'jan', 'feb', 'mar', 'april', 'may', 'june',
                    'july', 'aug', 'sep', 'oct', 'nov', 'dec']

            for i in header:
                if not i in df.columns:
                    df[i] = np.nan

            numeric = ['count', 'min', 'max', 'mean', 'sum', 'std', 'median', 'majority', 'minority',
                       'jan', 'feb', 'mar', 'april', 'may', 'june',
                    'july', 'aug', 'sep', 'oct', 'nov', 'dec']

            for i in numeric:
                df[i] = df[i].astype(float)

            # df.to_csv(os.path.join(dis_temp_dir_bands, "band{0}".format(str(band)), image_results), index=False)
            df_list.append(df)

        src.close()
        srci.close()

    final_df = pd.concat(df_list)
    print(final_df)
    final_df.to_csv(os.path.join(dis_temp_dir_bands, "{0}_{1}.csv".format(variable, im_date)), index=False)
    # final_df.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\non_rmb_fractional_cover_zonal_stats\{0}_test.csv".format(str(im_date)))
    # final_results = None
    return final_df

#
# def clean_data_frame_fn(output_list, output_dir, var_):
#     """ Create dataframe from output list, clean and export dataframe to a csv to export directory/max_temp sub-directory.
#
#     @param output_list: list object created by appending the final results list elements.
#     @param max_temp_output_dir: string object containing the path to the export directory/max_temp sub-directory .
#     @param complete_tile: string object containing the current Landsat tile information.
#     @return output_max_temp: dataframe object containing all max_temp zonal stats based on the ODK 1ha plots created
#     based on the current Landsat tile.
#     """
#
#     # convert the list to a pandas dataframe with a headers
#     headers = ['ident', 'site', 'im_date', var_ + '_mean', var_ + '_std', var_ + '_med', var_ + '_min',
#                var_ + '_max', var_ + '_count', var_ + "_p25", var_ + "_p50", var_ + "_p75", var_ + "_p95",
#                var_ + "_p99", var_ + "_rng", 'im_name']
#
#     output = pd.DataFrame.from_records(output_list, columns=headers)
#     # print('output_max_temp: ', output_max_temp)
#
#     # site = output_max_temp['site'].unique()
#     #
#     # print("length of site list: ", len(site))
#     # if len(site) >= 1:
#     #     for i in site:
#     #         out_df = output_max_temp[output_max_temp['site'] == i]
#     #
#     #         out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
#     #             str(i), var_))
#     #         # export the pandas df to a csv file
#     #         out_df.to_csv(out_path, index=False)
#
#     site = output['site'].unique()
#
#     print("length of site list: ", len(site))
#     if len(site) >= 1:
#         for i in site:
#             out_df = output[output['site'] == i]
#
#             out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
#                 str(i), var_))
#             # export the pandas df to a csv file
#             out_df.to_csv(out_path, index=False)
#
#     else:
#         out_path = os.path.join(output_dir, "{0}_{1}_zonal_stats.csv".format(
#             str(site), var_))
#         # export the pandas df to a csv file
#         output.to_csv(out_path, index=False)
#
#     return output


def main_routine(export_dir_path, variable, csv_file, temp_dir_path, geo_df, no_data):
    """ Calculate the zonal statistics for each 1ha site per QLD monthly max_temp image (single band).
    Concatenate and clean final output DataFrame and export to the Export directory/zonal stats.

    export_dir_path, zonal_stats_ready_dir, fpc_output_zonal_stats, fpc_complete_tile, i, csv_file, temp_dir_path, qld_dict"""

    print("Mosaic dka zonal stats beginning .........")
    print("no_data: ", no_data)

    uid = 'uid'
    output_list = []
    print("variable: ", variable)

    albers_dir = os.path.join(temp_dir_path, "albers")

    # # define the GCSWGS84 directory pathway
    # gcs_wgs84_dir = (temp_dir_path + '\\gcs_wgs84')
    #
    # define the max_tempOutput directory pathway
    output_dir = (os.path.join(export_dir_path, "{0}_zonal_stats".format(variable)))

    # call the project_shapefile_gcs_wgs84_fn function
    cgs_df, projected_shape_path = project_shapefile_gcs_wgs84_fn(albers_dir, geo_df)

    dka_temp_dir_bands = os.path.join(temp_dir_path, 'dka_temp_individual_bands')
    os.makedirs(dka_temp_dir_bands)

    # open the list of imagery and read it into memory and call the apply_zonal_stats_fn function
    with open(csv_file, 'r') as imagery_list:

        # loop through the list of imagery and input the image into the raster zonal_stats function
        for image in imagery_list:
            # print('image: ', image)

            image_s = image.rstrip()
            print("image_s: ", image_s)

            df_list = apply_zonal_stats_fn(image_s, projected_shape_path, uid, variable, no_data,
                                           dka_temp_dir_bands)  # cgs_df,projected_shape_path,

    all_files = glob(os.path.join(dka_temp_dir_bands,
                                  '*.csv'))
    # advisable to use os.path.join as this makes concatenation OS independent
    df_from_each_file = (pd.read_csv(f) for f in all_files)
    output_zonal_stats = pd.concat(df_from_each_file, ignore_index=False, axis=0, sort=False)
    print("-" * 50)
    print(output_zonal_stats.shape)
    print(output_zonal_stats.columns)
    print(output_zonal_stats)

    # -------------------------------------------------- Clean dataframe -----------------------------------------------
    #output_zonal_stats.to_csv(r"Z:\Scratch\Zonal_Stats_Pipeline\non_rmb_fractional_cover_zonal_stats\output_zonal_stats2.csv")
    # Convert the date to a time stamp
    #time_stamp_fn(output_zonal_stats)

    # remove 100 from zone_stats
    # landsat_correction_fn(output_zonal_stats, num_bands)

    # reshape the final dataframe
    output_zonal_stats = output_zonal_stats[
        ['uid', 'site', 'dka_image', 'date',
         'band', 'count', 'min', 'max', 'mean', 'sum', 'std', 'median', 'majority', 'minority', 'jan', 'feb', 'mar',
         'april', 'may', 'june', 'july', 'aug', 'sep', 'oct', 'nov', 'dec']]

    # uid_list = output_zonal_stats.uid.unique().tolist()
    # print("length of uid list: ", len(uid_list))
    # if len(uid_list) >= 1:
    #     for i in uid_list:
    #         out_df = output_zonal_stats[output_zonal_stats['uid'] == i]
    #
    #         out_path = os.path.join(export_dir_path, "{0}_zonal_stats".format(variable),
    #                                 "{0}_dka_zonal_stats.csv".format(str(i)))
    #         print("export to: ", out_path)
    #         # export the pandas df to a csv file
    #         out_df.to_csv(out_path, index=False)
    #
    #
    # else:
    #     out_path = os.path.join(export_dir_path, "{0}_zonal_stats".format(variable),
    #                             "{0}_dka_zonal_stats.csv".format(str(uid_list[0])))
    #     print("export to: ", out_path)
    #     # export the pandas df to a csv file
    #     output_zonal_stats.to_csv(out_path, index=False)

    site_list = output_zonal_stats.site.unique().tolist()
    print("length of site list: ", len(site_list))
    if len(site_list) >= 1:
        for i in site_list:
            out_df = output_zonal_stats[output_zonal_stats['site'] == i]

            out_path = os.path.join(export_dir_path, "{0}_zonal_stats".format(variable),
                                    "{0}_dka_zonal_stats.csv".format(str(i)))
            print("export to: ", out_path)
            # export the pandas df to a csv file
            out_df.to_csv(out_path, index=False)


    else:
        out_path = os.path.join(export_dir_path, "{0}_zonal_stats".format(variable),
                                "{0}_dka_zonal_stats.csv".format(str(site_list[0])))
        print("export to: ", out_path)
        # export the pandas df to a csv file
        output_zonal_stats.to_csv(out_path, index=False)

    # ----------------------------------------------- Delete temporary files -------------------------------------------
    # remove the temp dir and single band csv files
    shutil.rmtree(dka_temp_dir_bands)

    return projected_shape_path


if __name__ == "__main__":
    main_routine()
