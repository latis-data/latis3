# Standard modules
import os
import progressbar
import pandas as pd
from pandas import datetime
import numpy as np
from matplotlib import pyplot

# Custom modules
# import sys
# sys.path.append("..")
# sys.path.append(".")
# import nonparametric_dynamic_thresholding as ndt

__author__ = 'Shawn Polson'
__contact__ = 'shawn.polson@colorado.edu'


def parser(x):
    new_time = ''.join(x.split('.')[0])  # remove microseconds from time data
    try:
        return datetime.strptime(new_time, '%Y-%m-%d %H:%M:%S')  # for bus voltage, battery temp, wheel temp, and wheel rpm data
    except:
        return datetime.strptime(new_time, '%Y-%m-%d')  # for total bus current data


def detect_anomalies(ts, normal_model, var_name='Value', alg_name='Model_Algorithm', ds_name='Dataset', outlier_def='std', num_stds=2, ndt_errors=None,
                     plot_save_path=None, data_save_path=None, verbose=False):
    """Detect outliers in the time series data by comparing points against a "normal" model.

       Inputs:
           ts [pd Series]:           A pandas Series with a DatetimeIndex and a column for numerical values.
           normal_model [pd Series]: A pandas Series with a DatetimeIndex and a column for numerical values.

       Optional Inputs:
           var_name [str]:       The name of the dependent variable in the time series.
                                 Default is 'Value'.
           alg_name [str]:       The name of the algorithm used to create 'normal_model'.
                                 Default is 'Model_Algorithm'.
           ds_name [str]:        The name of the time series dataset.
                                 Default is 'Dataset'.
           outlier_def [str]:    {'std', 'errors', 'dynamic'} The definition of an outlier to be used. Can be 'std' for [num_stds] from the data's mean,
                                 'errors' for [num_stds] from the mean of the errors, or 'dynamic' for nonparametric dynamic thresholding
                                 Default is 'std'.
           num_stds [float]:     The number of standard deviations away from the mean used to define point outliers (when applicable).
                                 Default is 2.
           ndt_errors [list]:    Optionally skip nonparametric dynamic thresholding's 'get_errors()' and use these values instead.
           plot_save_path [str]: The file path (ending in file name *.png) for saving plots of outliers.
           data_save_path [str]: The file path (ending in file name *.csv) for saving CSVs with outliers.
           verbose [bool]:       When True, a progress bar will be displayed.

       Outputs:
           time_series_with_outliers [pd DataFrame]: A pandas DataFrame with a DatetimeIndex, two columns for numerical values, and an Outlier column (True or False).

       Optional Outputs:
           None

       Example:
           time_series_with_outliers = detect_anomalies(time_series, model, 'BatteryTemperature', 'Temperature (C)',
                                                        'ARIMA', 'dynamic', plot_path, data_path)
    """

    X = ts.values
    Y = normal_model.values
    outliers = pd.Series()
    errors = pd.Series()
    time_series_with_outliers = pd.DataFrame({var_name: ts, alg_name: normal_model})
    time_series_with_outliers['Outlier'] = False
    column_names = [var_name, alg_name, 'Outlier']  # column order
    time_series_with_outliers = time_series_with_outliers.reindex(columns=column_names)  # sort columns in specified order

    if verbose:
        # Start a progress bar
        widgets = [progressbar.Percentage(), progressbar.Bar(), progressbar.Timer(), ' ', progressbar.AdaptiveETA()]
        progress_bar_sliding_window = progressbar.ProgressBar(
            widgets=[progressbar.FormatLabel('Outliers (' + ds_name + ')')] + widgets,
            maxval=int(len(X))).start()

    # Define outliers by distance from "normal" model
    if outlier_def == 'std':
        # Label outliers using standard deviations
        std = float(X.std(ddof=0))
        outlier_points = []
        outlier_indices = []
        for t in range(len(X)):
            obs = X[t]
            y = Y[t]
            error = abs(y - obs)
            if error > std*num_stds:
                time_series_with_outliers.at[ts.index[t], 'Outlier'] = True
                outlier_points.append(obs)
                outlier_indices.append(ts.index[t])
            if verbose:
                progress_bar_sliding_window.update(t)  # advance progress bar
        outliers = outliers.append(pd.Series(outlier_points, index=outlier_indices))

    # Define outliers by distance from mean of errors
    elif outlier_def == 'errors':
        # Populate errors
        error_points = []
        error_indices = []
        for t in range(len(X)):
            obs = X[t]
            y = Y[t]
            error = abs(y - obs)
            error_points.append(error)
            error_indices.append(ts.index[t])
            if verbose:
                progress_bar_sliding_window.update(t)  # advance progress bar
        errors = errors.append(pd.Series(error_points, index=error_indices))

        mean_of_errors = float(errors.values.mean())
        std_of_errors = float(errors.values.std(ddof=0))
        threshold = mean_of_errors + (std_of_errors*num_stds)

        # Label outliers using standard deviations from the errors' mean
        outlier_points = []
        outlier_indices = []
        for t in range(len(X)):
            obs = X[t]
            error = errors[t]
            if error > threshold:
                time_series_with_outliers.at[ts.index[t], 'Outlier'] = True
                outlier_points.append(obs)
                outlier_indices.append(ts.index[t])
            if verbose:
                progress_bar_sliding_window.update(t)  # advance progress bar
        outliers = outliers.append(pd.Series(outlier_points, index=outlier_indices))

    # Define outliers using JPL's nonparamatric dynamic thresholding technique
    # elif outlier_def == 'dynamic':
    #     if verbose:
    #         progress_bar_sliding_window.update(int(len(X))/2)  # start progress bar timer
    #     outlier_points = []
    #     outlier_indices = []
    #     if ndt_errors is not None:
    #         smoothed_errors = ndt_errors
    #     else:
    #         smoothed_errors = ndt.get_errors(X, Y)
    #
    #     # These are the results of the nonparametric dynamic thresholding
    #     E_seq, anom_scores = ndt.process_errors(X, smoothed_errors)
    #     if verbose:
    #         progress_bar_sliding_window.update(int(len(X)) - 1)  # advance progress bar timer
    #
    #     # Convert sets of outlier start/end indices into outlier points
    #     for anom in E_seq:
    #         start = anom[0]
    #         end = anom[1]
    #         for i in range(start, end+1):
    #             time_series_with_outliers.at[ts.index[i], 'Outlier'] = True
    #             outlier_points.append(X[i])
    #             outlier_indices.append(ts.index[i])
    #     outliers = outliers.append(pd.Series(outlier_points, index=outlier_indices))

    if plot_save_path is not None:
        # Plot anomalies
        ax = ts.plot(color='#192C87', title=ds_name + ' with ' + alg_name + ' Outliers', label=var_name, figsize=(14, 6))
        normal_model.plot(color='#0CCADC', label=alg_name, linewidth=1.5)
        if len(outliers) > 0:
            outliers.plot(color='red', style='.', label='Outliers')
        ax.set(xlabel='Time', ylabel=var_name)
        pyplot.legend(loc='best')

        # Save plot
        plot_dir = plot_save_path[:plot_save_path.rfind('/')+1]
        if not os.path.exists(plot_dir):
            os.makedirs(plot_dir)
        pyplot.savefig(plot_save_path, dpi=500)

    if data_save_path is not None:
        # Save data
        data_dir = data_save_path[:data_save_path.rfind('/')+1]
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        time_series_with_outliers.to_csv(data_save_path)

    return time_series_with_outliers
