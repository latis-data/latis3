# Standard modules
import os
import datetime
from math import sqrt
import pandas as pd
from pandas import datetime
from pandas import read_csv
import numpy as np
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import acf, pacf
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima_model import ARIMAResults
from sklearn.metrics import mean_squared_error

# Custom modules
# import grid_search_hyperparameters
# from grid_search_hyperparameters import grid_search_arima_params
# from grid_search_hyperparameters import grid_search_sarima_params
#import nonparametric_dynamic_thresholding as ndt

__author__ = 'Shawn Polson'
__contact__ = 'shawn.polson@colorado.edu'


def time_parser(x):
    # Assume the time is given in ms since 1970-01-01
    return pd.to_datetime(x, unit='ms', origin='unix')


def model_with_arima(ts, train_size, order, seasonal_order=(), seasonal_freq=None, trend=None,
                     grid_search=False, path_to_model=None, verbose=False,
                     col_name='ARIMA', ds_name='Dataset', var_name='Value', model_save_path=None,
                     plot_save_path=None, data_save_path=None):
    """Model a time series with an ARIMA forecast.

       Inputs:
           ts [Array[Array[float, float]]:  The time series data as an array of arrays.
           train_size [float]:              The percentage of data to use for training, as a float (e.g., 0.66).
           order [tuple]:                   The order hyperparameters (p,d,q) for this ARIMA model.


       Optional Inputs:
           seasonal_order [tuple]: The seasonal order hyperparameters (P,D,Q) for this SARIMA model. When specifying these, 'seasonal_freq' must also be given.
           seasonal_freq [int]:    The freq hyperparameter for this SARIMA model, i.e., the number of samples that make up one seasonal cycle.
           trend [str]:            The trend hyperparameter for an SARIMA model.
           grid_search [bool]:     When True, perform a grid search to set values for the 'order' and 'seasonal order' hyperparameters.
                                   Note this overrides any given (p,d,q)(P,D,Q) hyperparameter values. Default is False.
           path_to_model [str]:    Path to a *.pkl file of a trained (S)ARIMA model. When set, no training will be done because that model will be used.
           verbose [bool]:         When True, show ACF and PACF plots before grid searching, plot residual training errors after fitting the model,
                                   and print predicted v. expected values during outlier detection. TODO: mention plot w/ forecast & outliers once it's under an "if verbose"
           col_name [str]:         The name of the ARIMA column.
                                   Default is 'ARIMA'.
           var_name [str]:         The name of the dependent variable in the time series.
                                   Default is 'Value'.
           model_save_path [str]:  The path to the root directory where the ARIMA model can be saved.
           plot_save_path [str]:   The path to the root directory where a plot of the ARIMA prediction can be saved.
           data_save_path [str]:   The path to the root directory where the ARIMA predictions can be saved as a CSV.


       Outputs:
           ts_with_arima [pd DataFrame]:

       Optional Outputs:
           None

       Example:
           time_series_with_arima = model_with_arima(time_series, train_size=0.8, order=(12,0,0),
                                                                             seasonal_order=(0,1,0), seasonal_freq=365,
                                                                             verbose=False)
    """

    # Finalize ARIMA/SARIMA hyperparameters
    if grid_search and path_to_model is not None:
        raise ValueError('\'grid_search\' should be False when specifying a path to a pre-trained ARIMA model.')

    if (seasonal_freq is not None) and (len(seasonal_order) == 3) and (grid_search is False):
        seasonal_order = seasonal_order + (seasonal_freq,)  # (P,D,Q,freq)
    elif (seasonal_freq is not None) and (len(seasonal_order) != 3) and (grid_search is False):
        raise ValueError('\'seasonal_order\' must be a tuple of 3 integers when specifying a seasonal frequency and not grid searching.')
    elif (seasonal_freq is None) and (len(seasonal_order) == 3) and (grid_search is False):
        raise ValueError('\'seasonal_freq\' must be given when specifying a seasonal order and not grid searching.')

    if grid_search:
        if seasonal_freq is None:  # ARIMA grid search
            order = grid_search_arima_params(ts)
        else:  # SARIMA grid search
            order, seasonal_order, trend = grid_search_sarima_params(ts, seasonal_freq)

    # Train or load ARIMA/SARIMA model
    X = pd.DataFrame(data=ts, columns=['Time', var_name])
    X['Time'] = X['Time'].apply(lambda time: time_parser(time))  # convert times to datetimes
    X = X.set_index('Time')                                      # set the datetime column as the index
    X = X.squeeze()                                              # convert to a Series

    split = int(len(X) * train_size)
    train, test = X[0:split], X[split:len(X)]
    # threshold = float(train.values.std(ddof=0)) * 2.0  # TODO: 2stds; finalize/decide std scheme (pass it in?)

    if len(seasonal_order) < 4:
        trained_model = ARIMA(train, order=order)
    else:
        trained_model = SARIMAX(train, order=order, seasonal_order=seasonal_order, trend=trend)

    if path_to_model is not None:
        # load pre-trained model
        trained_model_fit = ARIMAResults.load(path_to_model)
    else:
        trained_model_fit = trained_model.fit(disp=0)

    # save the just-trained model
    if model_save_path is not None:
        try:
            current_time = str(datetime.now().strftime("%Y-%m-%dT%H-%M-%S"))
            filename = 'SARIMA_' + var_name + '_' + train_size + '_' + str(order) + '_' + str(seasonal_order) + '_' + current_time + '.pkl'
            if not os.path.exists(model_save_path):
                os.makedirs(model_save_path)
            if model_save_path.endswith('/'):
                filename = model_save_path + filename
            else:
                filename = model_save_path + '/' + filename
            trained_model_fit.save(filename)
        except Exception as e:
            print('Saving model failed:')
            print(e)

    # Forecast with the trained ARIMA/SARIMA model
    predictions = trained_model_fit.predict(start=1, end=len(X)-1)
    predict_index = pd.Index(X.index[1:len(X)])
    predictions_with_dates = pd.Series(predictions.values, index=predict_index)
    errors = pd.Series()

    # Plot the forecast
    if plot_save_path is not None:
        if len(seasonal_order) < 4:  # ARIMA title
            title_text = ds_name + ' with ' + str(order) + ' ARIMA Forecast'
        else:  # SARIMA title
            title_text = ds_name + ' with ' + str(order) + '_' + str(seasonal_order) + '_' + str(trend) + ' ARIMA Forecast'
        ax = X.plot(color='#192C87', title=title_text, label=var_name, figsize=(14, 6))
        if len(test) > 0:
            test.plot(color='#441594', label='Test Data')
        predictions_with_dates.plot(color='#0CCADC', label='ARIMA Forecast')
        ax.set(xlabel='Time', ylabel=var_name)
        pyplot.legend(loc='best')

        # save the plot before showing it
        plot_filename = ds_name + '_with_arima_' + str(train_size) + '.png'
        if plot_save_path.endswith('/'):
            plot_path = plot_save_path + ds_name + '/arima/plots/' + str(int(train_size*100)) + ' percent/'
        else:
            plot_path = plot_save_path + '/' + ds_name + '/arima/plots/' + str(int(train_size*100)) + ' percent/'
        if not os.path.exists(plot_path):
            os.makedirs(plot_path)
        pyplot.savefig(plot_path + plot_filename, dpi=500)

    # Fill with NaNs the prediction values that were differenced away
    num_elems_needed = len(X) - len(predictions_with_dates)
    for i in range(num_elems_needed):
        predictions_with_dates = pd.Series([float("NaN")]).append(predictions_with_dates)

    ts_with_arima = pd.DataFrame({col_name: predictions_with_dates.values, var_name: X.values}, index=range(len(X)))
    ts_with_arima.rename_axis('Time', axis='index', inplace=True)  # name index 'Time'
    column_names = [var_name, col_name]  # column order
    ts_with_arima = ts_with_arima.reindex(columns=column_names)  # sort columns in specified order

    # Save data to proper directory with encoded file name
    if data_save_path is not None:
        data_filename = ds_name + '_with_arima_' + str(train_size) + '.csv'
        if data_save_path.endswith('/'):
            data_path = data_save_path + ds_name + '/arima/data/' + str(int(train_size * 100)) + ' percent/'
        else:
            data_path = data_save_path + '/' + ds_name + '/arima/data/' + str(int(train_size * 100)) + ' percent/'
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        ts_with_arima.to_csv(data_path + data_filename)

    return ts_with_arima

# ----------- grid_search_hyperparameters ------------------------------------------------------------------------------
# Standard modules
# TODO: use a progressbar?
# import progressbar
from ast import literal_eval as make_tuple
import pandas as pd
from math import sqrt
import numpy as np
from statsmodels.tsa.arima_model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
from multiprocessing import cpu_count
from joblib import Parallel
from joblib import delayed
import warnings
from warnings import catch_warnings
from warnings import filterwarnings
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error
from matplotlib import pyplot
import statistics


#----Helper Functions-----------------------------------------------------------------------------------------

# Evaluate an ARIMA model for a given order (p,d,q)
def evaluate_arima_model(X, arima_order):
    # prepare training dataset
    train_size = int(len(X) * 0.66)  # TODO: specify size instead of hardcoding 0.66?
    train, test = X[0:train_size], X[train_size:]
    history = [x for x in train]
    # make predictions
    predictions = list()
    for t in range(len(test)):
        model = ARIMA(history, order=arima_order)
        model_fit = model.fit(disp=1)  # TODO: pass in verbose and put this under "if verbose" for disp=1 else 0?
        yhat = model_fit.forecast()[0]
        predictions.append(yhat)
        history.append(test[t])
    # calculate out of sample error
    error = mean_squared_error(test, predictions)
    return error


# create a set of sarima configs to try
def generate_sarima_configs(seasonal=[0]):
    models = list()
    # define config lists
    # TODO: increase range of these lists? These are defaults from: https://machinelearningmastery.com/how-to-grid-search-sarima-model-hyperparameters-for-time-series-forecasting-in-python/
    # TODO: log/binary searching? (doubling/halving while errors go down; think big jumps through U-shape until finding local minimum, then refining)
    p_params = [0, 1, 2]
    d_params = [0, 1]
    q_params = [0, 1, 2]
    t_params = ['n', 'c', 't', 'ct']
    P_params = [0, 1, 2]
    D_params = [0, 1]
    Q_params = [0, 1, 2]
    freq_params = seasonal
    # create config instances (1,296 of them in total, but many will error and will get discarded)
    for p in p_params:
        for d in d_params:
            for q in q_params:
                for t in t_params:
                    for P in P_params:
                        for D in D_params:
                            for Q in Q_params:
                                for m in freq_params:
                                    cfg = [(p,d,q), (P,D,Q,m), t]
                                    models.append(cfg)
    return models


# grid search configs
def get_cross_validation_scores(data, order_configs, parallel=False):  # TODO: parallel should be True, but it always crashes
    configs_with_scores = None
    if parallel:
        # execute configs in parallel
        executor = Parallel(n_jobs=cpu_count(), backend='multiprocessing')
        tasks = (delayed(score_model)(data, config) for config in order_configs)
        configs_with_scores = executor(tasks)
    else:
        configs_with_scores = [score_model(data, config) for config in order_configs]

    # remove empty results
    configs_with_scores = [r for r in configs_with_scores if (r[1] is not None and len(r[1])>0)]
    # sort configs by error, asc
    configs_with_scores.sort(key=lambda tup: float(statistics.mean(tup[1])))
    return configs_with_scores


# score a model, return None on failure
def score_model(data, config, debug=False):
    rmses = []

    # show all warnings and fail on exception if debugging
    if debug:
        rmses = nested_cross_validation(data, config)
    else:
        # one failure during model validation suggests an unstable config
        try:
            # never show warnings when grid searching, too noisy
            with catch_warnings():
                filterwarnings("ignore")
                rmses = nested_cross_validation(data, config)
        except Exception as e:
            print(e)
            error = None
    # check for an interesting result
    # if len(rmses) > 0:
    #     print(' > Model[%s] %s' % (str(config), str(rmses))) # TODO: "if verbose" or don't print
    return (config, rmses)


def nested_cross_validation(data, config, n_folds=5):
    # Split the data into n_folds+1 chunks
    data = pd.Series(data)
    # data.plot(color='blue', title='Holdout Data (before splitting into folds)') # TODO: delete me
    # pyplot.show()
    folds = []
    fold_size = len(data) / (n_folds+1)
    for i in range(n_folds+1):  # 0 through 5 when n_folds=5
        if i == n_folds:
            folds.append(pd.Series(data[i*fold_size:]))  # last fold gets any off-by-one remainder point
        else:
            folds.append(pd.Series(data[i*fold_size:(i*fold_size)+fold_size]))

    # I can trust that this logic splits the data into perfect folds
    # data.plot(color='black', title='Holdout Data (after splitting into folds)')  # TODO: delete me
    # folds[0].plot(color='blue')
    # folds[1].plot(color='green')
    # folds[2].plot(color='red')
    # folds[3].plot(color='purple')
    # folds[4].plot(color='orange')
    # folds[5].plot(color='pink')
    # pyplot.show()

    RMSEs = train_and_validate(folds, config)
    return RMSEs


def train_and_validate(folds, config):
    num_folds = len(folds)
    RMSEs = []

    for i in range(num_folds-1):  # 0 through 5 when num_folds=6
        num_training_folds = i+1
        training_folds = folds[:num_training_folds]
        training_data = pd.Series([])
        training_data = training_data.append(training_folds, verify_integrity=True)
        validation_data = folds[i+1]
        RMSEs.append(sarima_forecast_and_score(training_data, validation_data, config))

    return RMSEs


def sarima_forecast_and_score(training, validation, config):
    X = training.append(validation, verify_integrity=True)
    order = config[0]
    seasonal_order = config[1]
    trend = config[2]

    trained_model = SARIMAX(training, order=order, seasonal_order=seasonal_order, trend=trend, enforce_stationarity=False, enforce_invertibility=False)
    # print('Training with configs: ' + str(config))
    trained_model_fit = trained_model.fit(disp=1)

    predictions = trained_model_fit.predict(start=1, end=len(X)-1, typ='levels')
    predict_index = pd.Index(X.index[1:len(X)])
    predictions_with_index = pd.Series(predictions.values, index=predict_index)

    model_rmse = sqrt(mean_squared_error(X[1:len(X)], predictions_with_index))
    return model_rmse



#----Grid Search Functions------------------------------------------------------------------------------------

def grid_search_arima_params(ts):
    """Perform a grid search to return ARIMA hyperparameters (p,d,q) for the given time series.

       Inputs:
           ts [pd Series]: A pandas Series with a DatetimeIndex and a column for numerical values.

       Optional Inputs:
           None

       Outputs:
           order [tuple]: The order hyperparameters (p,d,q) for this ARIMA model.

       Optional Outputs:
           None

       Example:
           order = grid_search_arima_params(time_series)
       """

    warnings.filterwarnings("ignore")  # helps prevent junk from cluttering up console output
    # TODO: don't hardcode these values? pass them in? increase range for p_values and q_values?
    p_values = range(0, 9)
    d_values = range(0, 3)
    q_values = range(0, 6)
    # Evaluate combinations of p, d and q values for an ARIMA model
    dataset = ts.astype('float32')
    best_score, best_cfg = float("inf"), None
    for p in p_values:
        for d in d_values:
            for q in q_values:
                order = (p, d, q)
                try:
                    mse = evaluate_arima_model(dataset, order)
                    if mse < best_score:
                        best_score, best_cfg = mse, order
                    print('ARIMA%s MSE=%.3f' % (order, mse))
                except:
                    continue
    # print('Best ARIMA%s MSE=%.3f' % (best_cfg, best_score))
    order = best_cfg  # TODO: always returning the best score doesn't lead to constant overfitting, does it?
    return order


def grid_search_sarima_params(ts, freq):
    """Perform a grid search to return SARIMA hyperparameters (p,d,q)(P,D,Q,freq) and trend for the given time series.
       See: https://machinelearningmastery.com/how-to-grid-search-sarima-model-hyperparameters-for-time-series-forecasting-in-python/

       Inputs:
           ts [pd Series]: A pandas Series with a DatetimeIndex and a column for numerical values.
           freq [int]:     The freq hyperparameter for this SARIMA model, i.e., toohe number of time steps for a single seasonal period.

       Optional Inputs:
           None

       Outputs:
           order [tuple]:          The order hyperparameters (p,d,q) for this SARIMA model.
           seasonal_order [tuple]: The seasonal order hyperparameters (P,D,Q,freq) for this SARIMA model.
           trend [str]:            The trend hyperparameter for this SARIMA model.

       Optional Outputs:
           None

       Example:
           order, seasonal_order, trend = grid_search_sarima_params(time_series, seasonal_freq)
       """

    #trivial_data = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0, 90.0, 80.0, 70.0, 60.0, 50.0, 40.0, 30.0, 20.0]
    data = ts.values
    holdout_size = 0.2  # TODO: try 0.5?
    split = int(len(data) * (1-holdout_size))
    training_data = data[0:split]

    possible_order_configs = generate_sarima_configs([freq])

    configs_with_scores = get_cross_validation_scores(training_data, possible_order_configs)  # get cross validation scores for each order_config

    # TODO: don't print this?
    # print('\n' + '----------------------------------GRID SEARCHING COMPLETE------------------------------------------')
    # print('RESULTS:' + '\n')
    # for config_and_score in configs_with_scores:
    #     print(str(config_and_score))

    best_order_config = configs_with_scores[0][0]  # TODO: always returning the best score doesn't lead to constant overfitting, does it?

    order = best_order_config[0]
    seasonal_order = best_order_config[1]
    trend = best_order_config[2]

    return order, seasonal_order, trend




# ----------------------------------------------------------------------------------------------------------------------


# TODO: remove below
# if __name__ == "__main__":
# 
#     datasets = ['Data/BusVoltage.csv', 'Data/TotalBusCurrent.csv', 'Data/BatteryTemperature.csv',
#                 'Data/WheelTemperature.csv', 'Data/WheelRPM.csv']
#     var_names = ['Voltage (V)', 'Current (A)', 'Temperature (C)', 'Temperature (C)', 'RPM']
# 
#     hyperparams = [
#         {'order': (1, 0, 0), 'seasonal_order': (0, 1, 0), 'freq': 365, 'trend': 'c'},
#         {'order': (1, 0, 2), 'seasonal_order': (0, 1, 0), 'freq': 365, 'trend': 'c'},
#         {'order': (0, 1, 0), 'seasonal_order': (0, 1, 0), 'freq': 365, 'trend': 'n'},
#         {'order': (1, 0, 0), 'seasonal_order': (0, 1, 0), 'freq': 365, 'trend': 'c'},
#         {'order': (1, 0, 0), 'seasonal_order': (0, 1, 0), 'freq': 365, 'trend': 'c'}
#     ]
# 
#     # 50% ARIMAs
#     for ds in range(len(datasets)):
#         dataset = datasets[ds]
#         var_name = var_names[ds]
#         ds_name = dataset[5:-4]  # drop 'Data/' and '.csv'
#         order = hyperparams[ds]['order']
#         seasonal_order = hyperparams[ds]['seasonal_order']
#         freq = hyperparams[ds]['freq']
#         trend = hyperparams[ds]['trend']
#         print('dataset: ' + dataset)
# 
#         # Load the dataset
#         time_series = pd.read_csv(dataset, header=0, parse_dates=[0], index_col=0, squeeze=True, date_parser=parser)
# 
#         # Daily average dataset
#         time_series = time_series.resample('24H').mean()
# 
#         # Use custom module 'model_with_arima' which also saves plots and data
#         blah = model_with_arima(time_series, ds_name=ds_name, train_size=0.5, order=order, seasonal_order=seasonal_order,
#                                 seasonal_freq=freq, trend=trend,
#                                 var_name=var_name, verbose=True)
# 
#     # 100% ARIMAs
#     for ds in range(len(datasets)):
#         dataset = datasets[ds]
#         var_name = var_names[ds]
#         ds_name = dataset[5:-4]  # drop 'Data/' and '.csv'
#         order = hyperparams[ds]['order']
#         seasonal_order = hyperparams[ds]['seasonal_order']
#         freq = hyperparams[ds]['freq']
#         trend = hyperparams[ds]['trend']
#         print('dataset: ' + dataset)
# 
#         # Load the dataset
#         time_series = pd.read_csv(dataset, header=0, parse_dates=[0], index_col=0, squeeze=True, date_parser=parser)
# 
#         # Daily average dataset
#         time_series = time_series.resample('24H').mean()
# 
#         # Use custom module 'model_with_arima' which also saves plots and data
#         blah = model_with_arima(time_series, ds_name=ds_name, train_size=1, order=order, seasonal_order=seasonal_order,
#                                 seasonal_freq=freq, trend=trend,
#                                 var_name=var_name, verbose=True)