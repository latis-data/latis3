# Standard modules
import os
import progressbar
import pandas as pd
import numpy as np
from matplotlib import pyplot


__author__ = 'Shawn Polson'
__contact__ = 'shawn.polson@colorado.edu'


def parser(x):
    # return the current time instead of parsing dates, for now
    return pd.datetime.today()


def model_with_rolling_mean(ts, window, ds_name, var_name='Value', verbose=False, calc_errors=False):
    """Model the time series data with a rolling mean.

       Inputs:
           ts [Vector[Array[int, float]]: 
           ---------------------ts [str]:       A string path to the time series data. Data is read as a pandas Series with a DatetimeIndex and a column for numerical values.
           window [int]:   Window size; the number of samples to include in the rolling mean.
           ds_name [str]:  Name of the dataset {bus voltage, etc.}

       Optional Inputs:
           var_name [str]:     The name of the dependent variable in the time series.
                               Default is 'Value'.
           verbose [bool]:     When True, a plot of the rolling mean will be displayed.
           calc_errors [bool]: Whether or not to calculate and return errors between data and rolling mean.

       Outputs:
           rolling_mean [pd Series]: The rolling mean, as a pandas Series with a DatetimeIndex and a column for the rolling mean.

       Optional Outputs:
           errors [pd Series]: The errors at each point, as a pandas Series with a DatetimeIndex and a column for the errors.

       Example:
           rolling_mean = detect_anomalies_with_rolling_mean(time_series, window_size, 'BusVoltage', False)
    """

    if window <= 0:
        raise ValueError('\'window\' must be given a value greater than 0 when using rolling mean.')

    # Gather statistics
    # === TESTING ========================
    #ts.reshape(1001,2)
    #print("Trying to print ts:")
    #print(ts)
    #return ts[1000]
    #ts = pd.read_csv(ts, header=0, parse_dates=[0], index_col=0, squeeze=True, date_parser=parser)  # This is ORIG
    ts = pd.DataFrame(data=ts, columns=['t', var_name])
    ts['t'] = ts['t'].apply(lambda time: str(time)[:-2])       # remove the ".0" from doubles
    ts['Time'] = '1970-01-01 00:00:00.' + ts['t'].astype(str)  # make proper time strings from the int index values
    ts = ts.drop(['t'], axis=1)                                # remove the intermediate "t" column
    ts['Time'] = pd.to_datetime(ts['Time'])                    # convert to a datetime
    ts = ts.set_index('Time')                                  # set the datetime as the index
    ts = pd.Series(ts[var_name].values, index=ts.index)        # convert to a series now that it's just index -> value
    #ts.to_csv("./save/PANDAS-DATAFRAME-FROM-JEP.csv")          # save to a csv to inspect (unfortunate way to do this)
    # ====================================
    rolling_mean = ts.rolling(window=window, center=False).mean() #TODO: NEEDS TO BE A SERIES
    first_window_mean = ts.iloc[:window].mean()
    for i in range(window):  # fill first 'window' samples with mean of those samples
        rolling_mean[i] = first_window_mean
    X = ts
    # === TESTING ========================
    #X.to_csv("./save/PANDAS-DATAFRAME-FROM-JEP-X.csv")                       # save to a csv to inspect
    #rolling_mean.to_csv("./save/PANDAS-DATAFRAME-FROM-JEP-ROLLINGMEAN.csv")  # save to a csv to inspect
    # ====================================
    rolling_mean = pd.Series(rolling_mean, index=ts.index)
    errors = pd.Series()

    # Save data to proper directory with encoded file name
    ts_with_rolling_mean = pd.DataFrame({'Rolling Mean': rolling_mean, var_name: ts})
    ts_with_rolling_mean.rename_axis('Time', axis='index', inplace=True)  # name index 'Time'
    column_names = [var_name, 'Rolling Mean']  # column order
    ts_with_rolling_mean = ts_with_rolling_mean.reindex(columns=column_names)  # sort columns in specified order

    data_filename = ds_name + '_with_rolling_mean.csv'
    data_path = './save/datasets/' + ds_name + '/rolling mean/data/'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    ts_with_rolling_mean.to_csv(data_path + data_filename)

    # Save plot to proper directory with encoded file name
    ax = ts.plot(color='#192C87', title=ds_name + ' with Rolling Mean', label=var_name, figsize=(14, 6))
    rolling_mean.plot(color='#0CCADC', label='Rolling Mean', linewidth=2.5)  #61AEFF is a nice baby blue
    ax.set(xlabel='Time', ylabel=var_name)
    pyplot.legend(loc='best')

    plot_filename = ds_name + '_with_rolling_mean.png'
    plot_path = './save/datasets/' + ds_name + '/rolling mean/plots/'
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    pyplot.savefig(plot_path + plot_filename, dpi=500)

    if verbose:
        pyplot.show()

    if calc_errors:
        # Start a progress bar
        widgets = [progressbar.Percentage(), progressbar.Bar(), progressbar.Timer(), ' ', progressbar.AdaptiveETA()]
        progress_bar_sliding_window = progressbar.ProgressBar(
            widgets=[progressbar.FormatLabel('Rolling Mean errors ')] + widgets,
            maxval=int(len(X))).start()

        # Get errors
        for t in range(len(X)):
            obs = X[t]
            y = rolling_mean[t]
            error = abs(y-obs)
            error_point = pd.Series(error, index=[ts.index[t]])
            errors = errors.append(error_point)

            progress_bar_sliding_window.update(t)  # advance progress bar

        return rolling_mean, errors
    else:
        #return rolling_mean
        return ts_with_rolling_mean
